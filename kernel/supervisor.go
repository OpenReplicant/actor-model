// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"strings"
	"sync"
	"time"
)

// SupervisorActor watches supervised actors and restarts them (with backoff)
// when they fail. It is itself an actor, subscribing to supervision.>.
//
// Failure is reported by publishing to supervision.failure.<actorID>. On
// restart the supervisor re-establishes the actor's bus subscription
// (reg.Start) and publishes supervision.restarted.<actorID>. If the restart
// fails it publishes supervision.restart-failed.<actorID> with the error.
//
// Use Watch(actor) to begin supervising a running actor and Unwatch(id) to
// stop. The kernel Actor interface is unchanged — restart re-establishes the
// actor's bus subscription; restarting an actor's external workload (a
// process, container, ...) is that actor's own concern when it receives its
// start command.
//
// Failure *detection* (heartbeats, panic recovery, ...) is intentionally
// out of scope here: anything that can publish supervision.failure.<id> can
// drive the supervisor. This keeps the supervisor decoupled from how failure
// is noticed.
//
// Restart intensity is bounded. Without a limit, an actor that fails on every
// start is restarted forever: a crash loop that burns CPU and floods the bus
// with supervision traffic. MaxRestarts within RestartWindow caps that; when the
// cap is exceeded the supervisor stops restarting the actor and publishes
// supervision.abandoned.<actorID> so the failure is visible rather than silent.
type SupervisorActor struct {
	id  string
	reg *Registry
	pub Publish

	mu         sync.Mutex
	supervised map[string]Actor // actor ID -> actor
	attempts   map[string]int   // restart attempt count per actor
	restarting map[string]bool  // restart in flight for an actor
	backoff    func(attempt int) time.Duration
	abandoned  map[string]bool // actors given up on after exceeding the limit
	firstSeen  map[string]time.Time

	// MaxRestarts is how many restarts are allowed within RestartWindow before
	// the actor is abandoned. Zero means the default (5); negative means no
	// limit.
	MaxRestarts int
	// RestartWindow is the period over which MaxRestarts is counted.
	RestartWindow time.Duration
}

// NewSupervisorActor creates a supervisor that restarts actors on reg when
// they fail. DefaultBackoff is used unless Backoff is set.
func NewSupervisorActor(id string, reg *Registry) *SupervisorActor {
	return &SupervisorActor{
		id:            id,
		reg:           reg,
		pub:           reg.Pub(),
		supervised:    make(map[string]Actor),
		attempts:      make(map[string]int),
		restarting:    make(map[string]bool),
		abandoned:     make(map[string]bool),
		firstSeen:     make(map[string]time.Time),
		backoff:       defaultBackoff,
		MaxRestarts:   5,
		RestartWindow: 60 * time.Second,
	}
}

// defaultBackoff is an exponential backoff: 50ms, 100ms, 200ms, 400ms, ...,
// capped at 5s.
func defaultBackoff(attempt int) time.Duration {
	d := 50 * time.Millisecond
	for i := 1; i < attempt; i++ {
		d *= 2
		if d > 5*time.Second {
			d = 5 * time.Second
			break
		}
	}
	return d
}

// Watch begins supervising actor. The actor should already be started on the
// registry; Watch only registers it so it can be restarted on failure.
func (a *SupervisorActor) Watch(actor Actor) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.supervised[actor.ID()] = actor
	a.attempts[actor.ID()] = 0
	delete(a.restarting, actor.ID())
}

// Unwatch stops supervising the actor with the given id.
func (a *SupervisorActor) Unwatch(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.supervised, id)
	delete(a.attempts, id)
	delete(a.restarting, id)
}

// ID returns the supervisor's actor id.
func (a *SupervisorActor) ID() string { return a.id }

// Topics returns the topics the supervisor subscribes to: supervision.>.
func (a *SupervisorActor) Topics() []string { return []string{"supervision.>"} }

func (a *SupervisorActor) Handle(ctx context.Context, msg Msg) {
	if !strings.HasPrefix(msg.Subject, "supervision.failure.") {
		return
	}
	id := strings.TrimPrefix(msg.Subject, "supervision.failure.")
	if id == "" {
		return
	}
	a.restart(id)
}

func (a *SupervisorActor) restart(id string) {
	a.mu.Lock()
	actor, ok := a.supervised[id]
	if !ok {
		a.mu.Unlock()
		return // not supervised
	}
	if a.restarting[id] {
		a.mu.Unlock()
		return // restart already in flight
	}
	if a.abandoned[id] {
		a.mu.Unlock()
		return // already gave up; further failures are not retried
	}

	// Restart intensity: count attempts inside a rolling window. A crash loop
	// otherwise restarts forever, which looks like activity but makes no
	// progress.
	now := time.Now()
	if _, seen := a.firstSeen[id]; !seen {
		a.firstSeen[id] = now
	}
	if a.MaxRestarts >= 0 && a.AttemptsInWindow(id, now) >= a.maxRestarts() {
		a.abandoned[id] = true
		a.mu.Unlock()
		a.pub("supervision.abandoned."+id, []byte("restart intensity exceeded"))
		return
	}
	if now.Sub(a.firstSeen[id]) > a.window() {
		a.firstSeen[id] = now
		a.attempts[id] = 0
	}

	a.restarting[id] = true
	attempt := a.attempts[id] + 1
	a.attempts[id] = attempt
	delay := a.backoff(attempt)
	a.mu.Unlock()

	go func() {
		time.Sleep(delay)
		err := a.reg.Start(actor) // re-establish the actor's bus subscription

		a.mu.Lock()
		a.restarting[id] = false
		if err != nil {
			a.mu.Unlock()
			a.pub("supervision.restart-failed."+id, []byte(err.Error()))
			return
		}
		a.attempts[id] = 0 // reset on successful restart
		a.mu.Unlock()
		a.pub("supervision.restarted."+id, []byte(""))
	}()
}

// maxRestarts returns the configured limit, falling back to the default.
func (a *SupervisorActor) maxRestarts() int {
	if a.MaxRestarts == 0 {
		return 5
	}
	return a.MaxRestarts
}

// window returns the configured restart window, falling back to the default.
func (a *SupervisorActor) window() time.Duration {
	if a.RestartWindow <= 0 {
		return 60 * time.Second
	}
	return a.RestartWindow
}

// AttemptsInWindow reports how many restart attempts have been counted for id
// in its current window. Callers must hold a.mu (the test reads it under lock).
func (a *SupervisorActor) AttemptsInWindow(id string, now time.Time) int {
	if now.Sub(a.firstSeen[id]) > a.window() {
		return 0
	}
	return a.attempts[id]
}

// ensure SupervisorActor implements Actor at compile time.
var _ Actor = (*SupervisorActor)(nil)

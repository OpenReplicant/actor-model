package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// failActor is an actor that, when told to fail, publishes
// supervision.failure.<id> so the supervisor can restart it.
type failActor struct {
	id         string
	topics     []string
	reg        Publish
	mu         sync.Mutex
	running    bool
	startCount int
}

func (a *failActor) ID() string       { return a.id }
func (a *failActor) Topics() []string { return a.topics }

func (a *failActor) Handle(ctx context.Context, msg Msg) {
	switch msg.Subject {
	case a.id + ".fail":
		a.reg("supervision.failure."+a.id, []byte("boom"))
	case a.id + ".start":
		a.mu.Lock()
		a.running = true
		a.startCount++
		a.mu.Unlock()
	case a.id + ".stop":
		a.mu.Lock()
		a.running = false
		a.mu.Unlock()
	}
}

func (a *failActor) isRunning() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running
}

func (a *failActor) getStartCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.startCount
}

func TestSupervisorRestartsOnFailure(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	actor := &failActor{id: "svc", topics: []string{"svc.>"}, reg: reg.Pub()}
	if err := reg.Start(actor); err != nil {
		t.Fatalf("start actor: %v", err)
	}

	sup := NewSupervisorActor("sup", reg)
	if err := reg.Start(sup); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	sup.Watch(actor)

	cap := &recordingActor{id: "cap", topics: []string{"supervision.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Simulate failure.
	if err := reg.Publish("svc.fail", []byte("boom")); err != nil {
		t.Fatalf("publish fail: %v", err)
	}

	// Restarted message should appear.

	// Restarted message should appear.
	if !waitFor(t, 5*time.Second, func() bool {
		return contains(cap.subjects(), "supervision.restarted.svc")
	}) {
		t.Fatalf("expected restarted message, got %v", cap.subjects())
	}

	// The actor should be re-subscribed: send it a start command and confirm it
	// received it (reg.Start re-established its bus subscription).
	if err := reg.Publish("svc.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return actor.isRunning()
	}) {
		t.Fatalf("expected actor to be re-subscribed and handling start")
	}

	// Attempt counter reset after successful restart.
	if got := actor.getStartCount(); got != 1 {
		t.Fatalf("expected startCount 1 after restart, got %d", got)
	}
}

func TestSupervisorIgnoresUnsupervisedActor(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	sup := NewSupervisorActor("sup2", reg)
	if err := reg.Start(sup); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}

	cap := &recordingActor{id: "cap2", topics: []string{"supervision.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// No actor is supervised for "orphan", so failure is ignored.
	if err := reg.Publish("supervision.failure.orphan", []byte("boom")); err != nil {
		t.Fatalf("publish failure: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	// The failure message itself is just on the bus; the supervisor must not
	// restart an unsupervised actor, so no restarted/restart-failed message.
	for _, s := range cap.subjects() {
		if s == "supervision.restarted.orphan" || s == "supervision.restart-failed.orphan" {
			t.Fatalf("expected no restart for unsupervised actor, got %v", cap.subjects())
		}
	}
}

func TestSupervisorUnwatchStopsRestart(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	actor := &failActor{id: "svc3", topics: []string{"svc3.>"}, reg: reg.Pub()}
	if err := reg.Start(actor); err != nil {
		t.Fatalf("start actor: %v", err)
	}

	sup := NewSupervisorActor("sup3", reg)
	if err := reg.Start(sup); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	sup.Watch(actor)
	sup.Unwatch("svc3") // stop supervising immediately

	cap := &recordingActor{id: "cap3", topics: []string{"supervision.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	if err := reg.Publish("svc3.fail", []byte("boom")); err != nil {
		t.Fatalf("publish fail: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	// After unwatch, the failure message is on the bus but no restart happens.
	for _, s := range cap.subjects() {
		if s == "supervision.restarted.svc3" || s == "supervision.restart-failed.svc3" {
			t.Fatalf("expected no restart after unwatch, got %v", cap.subjects())
		}
	}
}

func TestSupervisorBackoffResetsOnSuccess(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	actor := &failActor{id: "svc4", topics: []string{"svc4.>"}, reg: reg.Pub()}
	if err := reg.Start(actor); err != nil {
		t.Fatalf("start actor: %v", err)
	}

	sup := NewSupervisorActor("sup4", reg)
	if err := reg.Start(sup); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	sup.Watch(actor)

	cap := &recordingActor{id: "cap4", topics: []string{"supervision.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Two failures in quick succession -> two restarts, both succeed.
	if err := reg.Publish("svc4.fail", []byte("boom")); err != nil {
		t.Fatalf("publish fail 1: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return contains(cap.subjects(), "supervision.restarted.svc4")
	}) {
		t.Fatalf("expected first restarted, got %v", cap.subjects())
	}
	if err := reg.Publish("svc4.fail", []byte("boom")); err != nil {
		t.Fatalf("publish fail 2: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return countOf(cap.subjects(), "supervision.restarted.svc4") >= 2
	}) {
		t.Fatalf("expected second restarted, got %v", cap.subjects())
	}
}

// countOf counts how many times target appears in subjects.
func countOf(subjects []string, target string) int {
	n := 0
	for _, s := range subjects {
		if s == target {
			n++
		}
	}
	return n
}

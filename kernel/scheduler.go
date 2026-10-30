package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// SchedulerActor is the loop that turns placement advice into action.
//
// Until it existed, ownership and placement were two halves of a decision with
// nothing connecting them: a lease said who may run an actor, a policy said
// where it belongs, and a human started it there. The scheduler closes that:
//
//	desired state ──▶ placement ──▶ is it mine? ──▶ start / stop / ignore
//
// It subscribes to scheduler.plan.> for explicit plans and also reconciles the
// desired set on a tick, so an actor declared in config is started without
// anyone publishing anything.
//
// Three rules make it safe:
//
//   - It only starts an actor the placement names *this* node for. Starting
//     elsewhere would defeat the ownership lease.
//   - It is idempotent: an actor already running here is left alone. A loop
//     that restarted on every tick would destroy in-memory state continuously.
//   - It stops an actor this node runs when a later plan places it elsewhere,
//     so ownership never has two live holders during a move.
//
// It does not decide placement itself and does not fight the lease: if Start
// fails because another node holds the lease, that is reported, not retried in
// a tight loop.
type SchedulerActor struct {
	id     string
	topics []string
	reg    *Registry
	reglet SchedulerRegistry
	policy Placement
	node   string
	pub    Publish

	// ReconcileInterval is how often the desired set is re-evaluated. Zero
	// disables the tick (explicit plans still work).
	ReconcileInterval time.Duration

	mu      sync.Mutex
	desired map[string]string        // actor id -> plugin
	running map[string]bool          // actor ids this scheduler started
	plans   map[string]SchedulerPlan // last applied plan per actor
	stopCh  chan struct{}
	once    sync.Once
}

// SchedulerRegistry is what the scheduler needs from the runtime: the ability
// to start and stop actors and to see what is running. *Registry satisfies it
// through RegistrySchedulerRegistry.
type SchedulerRegistry interface {
	// StartActor starts the named actor using the given plugin and subscribing
	// to topics. An empty topics slice means the implementation's default.
	StartActor(id, plugin string, topics []string) error
	// StopActor stops the named actor.
	StopActor(id string) error
	// Running returns the ids currently running under this registry.
	Running() []string
}

// registryProvider is implemented by SchedulerRegistry implementations that can
// expose their underlying Registry. The scheduler uses it for the cluster view
// and for publishing, but works without it (single-node, no reporting).
type registryProvider interface {
	Registry() *Registry
}

// SchedulerPlan is the placement decision for one actor, as published on
// scheduler.plan.<actor> and echoed on scheduler.applied.<actor>.
type SchedulerPlan struct {
	// Node is the node placement chose. Empty means "not yet decided".
	Node string `json:"node"`
	// Plugin is the plugin to instantiate the actor from.
	Plugin string `json:"plugin,omitempty"`
	// Topics are the subjects the started actor subscribes to. Empty means the
	// scheduler's convention (dyn.<actor>.>), which is a usable default but
	// rarely what the actor actually needs.
	Topics []string `json:"topics,omitempty"`
	// Requires are node labels the target must carry.
	Requires map[string]string `json:"requires,omitempty"`
	// Pin overrides placement and names a node explicitly.
	Pin string `json:"pin,omitempty"`
	// Reason explains the decision, for the dashboard.
	Reason string `json:"reason,omitempty"`
}

// SchedulerReporter is implemented by the scheduler actor. The dashboard asks
// the registry for it so scheduler state is a read, not a race with the
// scheduler's own actor.
//
// It lives in the kernel rather than beside the WebUI actor because the
// registry stores one (SetScheduler) regardless of which dashboard, if any,
// is running.
type SchedulerReporter interface {
	Plans() []SchedulerStatus
}

// NewSchedulerActor creates a scheduler running on node, using policy to decide
// placement and reglet to act.
func NewSchedulerActor(id string, reglet SchedulerRegistry, policy Placement, node string) *SchedulerActor {
	if policy == nil {
		policy = NewLeastLoadedPlacement()
	}
	s := &SchedulerActor{
		id: id,
		// Plans and on-demand lifecycle requests. It deliberately does not
		// subscribe to its own output (scheduler.applied.>/scheduler.error.>).
		topics:            []string{"scheduler.plan.>", "supervision.start.>", "supervision.stop.>"},
		reglet:            reglet,
		policy:            policy,
		node:              node,
		desired:           make(map[string]string),
		running:           make(map[string]bool),
		plans:             make(map[string]SchedulerPlan),
		stopCh:            make(chan struct{}),
		ReconcileInterval: 0,
	}
	if rp, ok := reglet.(registryProvider); ok {
		s.reg = rp.Registry()
		if s.reg != nil {
			s.pub = s.reg.Pub()
		}
	}
	return s
}

// RegistrySchedulerRegistry adapts a *Registry to SchedulerRegistry. Actor
// instances are constructed through a factory map supplied by the binary, since
// only it knows which plugins exist.
type RegistrySchedulerRegistry struct {
	reg     *Registry
	factory func(id, plugin string, topics []string) (Actor, error) // may be nil
	mu      sync.Mutex
	dynamic map[string]Actor // actors this adapter started
}

// Registry returns the underlying registry, satisfying registryProvider.
func (r *RegistrySchedulerRegistry) Registry() *Registry { return r.reg }

// NewRegistrySchedulerRegistry adapts reg, using factory to build actors for
// dynamic starts. A nil factory makes StartActor return an error.
func NewRegistrySchedulerRegistry(reg *Registry, factory func(id, plugin string, topics []string) (Actor, error)) *RegistrySchedulerRegistry {
	return &RegistrySchedulerRegistry{reg: reg, factory: factory, dynamic: make(map[string]Actor)}
}

// StartActor instantiates plugin and starts it on the registry.
func (r *RegistrySchedulerRegistry) StartActor(id, plugin string, topics []string) error {
	if r.factory == nil {
		return fmt.Errorf("scheduler: no actor factory configured for %q", plugin)
	}
	a, err := r.factory(id, plugin, topics)
	if err != nil {
		return err
	}
	if err := r.reg.Start(a); err != nil {
		return err
	}
	r.mu.Lock()
	r.dynamic[id] = a
	r.mu.Unlock()
	return nil
}

// StopActor stops an actor started by this adapter.
func (r *RegistrySchedulerRegistry) StopActor(id string) error {
	r.mu.Lock()
	a, ok := r.dynamic[id]
	delete(r.dynamic, id)
	r.mu.Unlock()
	if !ok {
		return nil
	}
	return r.reg.Stop(a)
}

// Running returns the ids the registry currently has boxes for.
func (r *RegistrySchedulerRegistry) Running() []string {
	r.reg.mu.Lock()
	defer r.reg.mu.Unlock()
	out := make([]string, 0, len(r.reg.actorBox))
	for id := range r.reg.actorBox {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ID returns the scheduler's actor id.
func (s *SchedulerActor) ID() string { return s.id }

// Topics returns the subjects the scheduler consumes. It deliberately does not
// subscribe to its own output (scheduler.applied.>/scheduler.error.>).
func (s *SchedulerActor) Topics() []string { return s.topics }

// Desire declares that actor should be running, instantiated from plugin. The
// reconcile loop then places and starts it. Declaring an actor that is already
// desired is a no-op.
func (s *SchedulerActor) Desire(id, plugin string) {
	s.DesireWithTopics(id, plugin, nil)
}

// DesireWithTopics declares an actor along with the subjects it should
// subscribe to. Topics carry through to the start call; without them the
// implementation falls back to its convention.
func (s *SchedulerActor) DesireWithTopics(id, plugin string, topics []string) {
	s.mu.Lock()
	s.desired[id] = plugin
	plan := s.plans[id]
	if len(topics) > 0 {
		plan.Topics = topics
		s.plans[id] = plan
	}
	s.mu.Unlock()
	s.reconcile()
}

// Forget removes an actor from the desired set and stops it if this node runs
// it.
func (s *SchedulerActor) Forget(id string) {
	s.mu.Lock()
	delete(s.desired, id)
	_, running := s.running[id]
	delete(s.running, id)
	s.mu.Unlock()
	if running {
		if err := s.reglet.StopActor(id); err != nil {
			s.reportError(id, err)
		}
	}
}

// Handle applies an explicit plan, honours on-demand lifecycle requests, or
// nudges the reconcile loop.
func (s *SchedulerActor) Handle(_ context.Context, msg Msg) {
	switch {
	case strings.HasPrefix(msg.Subject, "supervision.start."):
		// On-demand start, used by the dashboard and by operators. The body may
		// carry {"plugin": "..."}; the default is echo so a bare publish works.
		id := strings.TrimPrefix(msg.Subject, "supervision.start.")
		if id == "" {
			return
		}
		var req struct {
			Plugin string   `json:"plugin"`
			Topics []string `json:"topics"`
		}
		if len(msg.Data) > 0 {
			_ = json.Unmarshal(msg.Data, &req) // tolerate an empty or non-JSON body
		}
		if req.Plugin == "" {
			req.Plugin = "echo"
		}
		s.DesireWithTopics(id, req.Plugin, req.Topics)

	case strings.HasPrefix(msg.Subject, "supervision.stop."):
		id := strings.TrimPrefix(msg.Subject, "supervision.stop.")
		if id == "" {
			return
		}
		s.Forget(id)

	case strings.HasPrefix(msg.Subject, "scheduler.plan."):
		id := strings.TrimPrefix(msg.Subject, "scheduler.plan.")
		if id == "" {
			return
		}
		var plan SchedulerPlan
		if len(msg.Data) > 0 {
			if err := json.Unmarshal(msg.Data, &plan); err != nil {
				s.reportError(id, fmt.Errorf("invalid plan: %w", err))
				return
			}
		}
		s.apply(id, plan)
	}
}

// apply acts on one plan for one actor.
func (s *SchedulerActor) apply(id string, plan SchedulerPlan) {
	if plan.Node == "" {
		// No explicit destination: ask placement.
		node, reason, err := s.place(id, plan)
		if err != nil {
			s.reportError(id, err)
			return
		}
		plan.Node, plan.Reason = node, reason
	}

	s.mu.Lock()
	alreadyRunning := s.running[id]
	s.plans[id] = plan
	if plan.Plugin != "" {
		s.desired[id] = plan.Plugin
	}
	s.mu.Unlock()

	if plan.Node != s.node {
		// Placed elsewhere. If we are running it, stop: ownership must not have
		// two holders even briefly.
		if alreadyRunning {
			s.mu.Lock()
			delete(s.running, id)
			s.mu.Unlock()
			if err := s.reglet.StopActor(id); err != nil {
				s.reportError(id, err)
				return
			}
			s.report(id, plan, "stopped: placed on "+plan.Node)
		}
		return
	}

	// Placed here. Start only if not already running: a reconcile tick that
	// restarted running actors would wipe their in-memory state every tick.
	if alreadyRunning {
		s.mu.Lock()
		s.running[id] = true
		s.mu.Unlock()
		return
	}
	if err := s.reglet.StartActor(id, plan.Plugin, plan.Topics); err != nil {
		s.reportError(id, err)
		return
	}
	s.mu.Lock()
	s.running[id] = true
	s.mu.Unlock()
	s.report(id, plan, "started")
}

// place asks the policy where the actor belongs.
func (s *SchedulerActor) place(id string, plan SchedulerPlan) (node, reason string, err error) {
	if s.reg == nil {
		// No cluster view available: this node is the only candidate.
		return s.node, "single-node (no cluster view)", nil
	}
	leases, _, enabled := s.reg.Ownership()
	if !enabled {
		return s.node, "ownership disabled: local only", nil
	}
	node, err = s.policy.Place(context.Background(), PlacementRequest{
		Actor:    id,
		Pin:      plan.Pin,
		Requires: plan.Requires,
	}, NewLeasePlacementView(leases, nil))
	if err != nil {
		return "", "", err
	}
	return node, "least-loaded placement", nil
}

// reconcile re-evaluates the desired set.
func (s *SchedulerActor) reconcile() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.desired))
	for id := range s.desired {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	s.mu.Unlock()

	for _, id := range ids {
		s.mu.Lock()
		plugin := s.desired[id]
		plan := s.plans[id]
		s.mu.Unlock()
		plan.Plugin = plugin
		s.apply(id, plan)
	}
}

// Start launches the reconcile loop when an interval is configured.
func (s *SchedulerActor) Start() {
	s.once.Do(func() {
		if s.ReconcileInterval <= 0 {
			return
		}
		go func() {
			ticker := time.NewTicker(s.ReconcileInterval)
			defer ticker.Stop()
			for {
				select {
				case <-s.stopCh:
					return
				case <-ticker.C:
					s.reconcile()
				}
			}
		}()
	})
}

// Stop halts the reconcile loop.
func (s *SchedulerActor) Stop() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
}

// Plans returns the last applied plan per actor, for the dashboard.
func (s *SchedulerActor) Plans() []SchedulerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SchedulerStatus, 0, len(s.desired))
	for id, plugin := range s.desired {
		plan := s.plans[id]
		out = append(out, SchedulerStatus{
			Actor:   id,
			Plugin:  plugin,
			Node:    plan.Node,
			Running: s.running[id],
			Local:   plan.Node == s.node,
			Reason:  plan.Reason,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Actor < out[j].Actor })
	return out
}

// SchedulerStatus is one row of scheduler state, for the dashboard.
type SchedulerStatus struct {
	Actor   string `json:"actor"`
	Plugin  string `json:"plugin"`
	Node    string `json:"node"`
	Running bool   `json:"running"`
	Local   bool   `json:"local"` // running on this node
	Reason  string `json:"reason,omitempty"`
}

// report publishes the applied plan so the decision is visible.
func (s *SchedulerActor) report(id string, plan SchedulerPlan, action string) {
	if s.pub == nil {
		return
	}
	body, _ := json.Marshal(map[string]interface{}{
		"actor":  id,
		"plan":   plan,
		"action": action,
		"node":   s.node,
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
	_ = s.pub("scheduler.applied."+id, body)
}

// reportError publishes a failed decision.
func (s *SchedulerActor) reportError(id string, err error) {
	slog.Error("scheduler", "actor", id, "node", s.node, "err", err)
	if s.pub == nil {
		return
	}
	body, _ := json.Marshal(map[string]interface{}{
		"actor": id,
		"node":  s.node,
		"error": err.Error(),
		"time":  time.Now().UTC().Format(time.RFC3339),
	})
	_ = s.pub("scheduler.error."+id, body)
}

// ensure SchedulerActor implements Actor at compile time.
var _ Actor = (*SchedulerActor)(nil)

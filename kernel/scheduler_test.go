package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// schedulerReglet is a minimal SchedulerRegistry implementation backed by a
// real Registry, so the scheduler is tested against the same lease and start
// paths the binary uses.
type schedulerReglet struct {
	reg      *Registry
	started  map[string]int
	stopped  map[string]int
	startErr error
	mu       chan struct{} // 1-slot mutex so the test helper needs no sync import
}

func newSchedulerReglet(reg *Registry) *schedulerReglet {
	return &schedulerReglet{
		reg:     reg,
		started: make(map[string]int),
		stopped: make(map[string]int),
		mu:      make(chan struct{}, 1),
	}
}

func (s *schedulerReglet) lock()   { s.mu <- struct{}{} }
func (s *schedulerReglet) unlock() { <-s.mu }

func (s *schedulerReglet) StartActor(id, plugin string, topics []string) error {
	if s.startErr != nil {
		return s.startErr
	}
	if len(topics) == 0 {
		topics = []string{"dyn." + id + ".>"}
	}
	s.lock()
	s.started[id]++
	s.unlock()
	return s.reg.Start(&recordingActor{id: id, topics: topics})
}

func (s *schedulerReglet) StopActor(id string) error {
	s.lock()
	s.stopped[id]++
	s.unlock()
	return nil
}

// Registry exposes the underlying registry so the scheduler can see the
// cluster view and publish reports.
func (s *schedulerReglet) Registry() *Registry { return s.reg }

func (s *schedulerReglet) Running() []string {
	out := []string{}
	s.lock()
	for id := range s.reg.actorBox {
		out = append(out, id)
	}
	s.unlock()
	return out
}

func (s *schedulerReglet) startCount(id string) int {
	s.lock()
	defer s.unlock()
	return s.started[id]
}

// TestSchedulerStartsOnOwningNode: the node named by placement.plan issues the
// start; every other node ignores it.
func TestSchedulerStartsOnOwningNode(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	// A plan naming node-a should start the actor here.
	if err := reg.Publish("scheduler.plan.work-1", []byte(`{"node":"node-a","plugin":"echo"}`)); err != nil {
		t.Fatalf("publish plan: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return reglet.startCount("work-1") == 1 }) {
		t.Fatal("scheduler did not start an actor planned for its own node")
	}
}

// TestSchedulerIgnoresOtherNodesPlans is the anti-duplication property: a node
// must not run an actor placed elsewhere, or placement would defeat ownership.
func TestSchedulerIgnoresOtherNodesPlans(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	if err := reg.Publish("scheduler.plan.work-2", []byte(`{"node":"node-b","plugin":"echo"}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := reglet.startCount("work-2"); got != 0 {
		t.Fatalf("node-a started an actor planned for node-b (%d starts)", got)
	}
}

// TestSchedulerIsIdempotent: repeated plans for an actor already running here
// must not restart it. A scheduler that restarts on every reconcile tick would
// destroy in-memory actor state continuously.
func TestSchedulerIsIdempotent(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := reg.Publish("scheduler.plan.work-3", []byte(`{"node":"node-a","plugin":"echo"}`)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	time.Sleep(400 * time.Millisecond)
	if got := reglet.startCount("work-3"); got != 1 {
		t.Fatalf("expected exactly 1 start, got %d (scheduler is not idempotent)", got)
	}
}

// TestSchedulerStopsWhenPlacedElsewhere: if a plan moves an actor to another
// node, the old owner must stop it. Otherwise ownership would flip while both
// sides keep consuming.
func TestSchedulerStopsWhenPlacedElsewhere(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	if err := reg.Publish("scheduler.plan.work-4", []byte(`{"node":"node-a","plugin":"echo"}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return reglet.startCount("work-4") == 1 }) {
		t.Fatal("actor never started")
	}

	// Now placement moves it away.
	if err := reg.Publish("scheduler.plan.work-4", []byte(`{"node":"node-b","plugin":"echo"}`)); err != nil {
		t.Fatalf("publish move: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		reglet.lock()
		defer reglet.unlock()
		return reglet.stopped["work-4"] == 1
	}) {
		t.Fatal("scheduler did not stop an actor that moved to another node")
	}
}

// TestSchedulerReconcileUsesPlacement: the reconcile tick asks the placement
// policy where each desired actor belongs, so a newly added actor is started
// without anyone publishing an explicit plan.
func TestSchedulerReconcileUsesPlacement(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	sched.ReconcileInterval = 100 * time.Millisecond
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	// Declare a desired actor. With no leases held by other nodes, the only
	// node in the view is this one, so placement picks node-a.
	sched.Desire("reconciled-1", "echo")
	if !waitFor(t, 3*time.Second, func() bool { return reglet.startCount("reconciled-1") == 1 }) {
		t.Fatal("reconcile loop did not start the desired actor")
	}
}

// TestSchedulerReportsPlan: the scheduler publishes the plan it acted on, so the
// dashboard and an operator can see the decision rather than infer it.
func TestSchedulerReportsPlan(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	cap := &recordingActor{id: "plan-capture", topics: []string{"scheduler.applied.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start capture: %v", err)
	}

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	if err := reg.Publish("scheduler.plan.work-5", []byte(`{"node":"node-a","plugin":"echo"}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "scheduler.applied.work-5")
	}) {
		t.Fatalf("expected scheduler.applied.work-5, got %v", cap.subjects())
	}
}

// TestSchedulerSurfacesStartErrors: a failed start must be published, not
// swallowed, or an actor is silently absent from the cluster.
func TestSchedulerSurfacesStartErrors(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	cap := &recordingActor{id: "err-capture", topics: []string{"scheduler.error.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start capture: %v", err)
	}

	reglet := newSchedulerReglet(reg)
	reglet.startErr = errPlacementFailed
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	if err := reg.Publish("scheduler.plan.work-6", []byte(`{"node":"node-a","plugin":"echo"}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "scheduler.error.work-6")
	}) {
		t.Fatal("start failure was swallowed")
	}
}

// errPlacementFailed is a sentinel for the error-path test.
var errPlacementFailed = &schedulerTestError{"injected start failure"}

type schedulerTestError struct{ msg string }

func (e *schedulerTestError) Error() string { return e.msg }

// TestSchedulerHandlesDashboardLifecycle is a regression test for a real bug:
// the dashboard published supervision.start.<id> / supervision.stop.<id> but
// nothing subscribed to those subjects, so the buttons did nothing.
func TestSchedulerHandlesDashboardLifecycle(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	// What the dashboard sends: a subject plus an optional plugin.
	if err := reg.Publish("supervision.start.ui-1", []byte(`{"plugin":"echo"}`)); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return reglet.startCount("ui-1") == 1 }) {
		t.Fatal("supervision.start.<id> did not start an actor (dashboard button is inert)")
	}

	if err := reg.Publish("supervision.stop.ui-1", nil); err != nil {
		t.Fatalf("publish stop: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		reglet.lock()
		defer reglet.unlock()
		return reglet.stopped["ui-1"] == 1
	}) {
		t.Fatal("supervision.stop.<id> did not stop the actor")
	}
}

// TestSchedulerStartCarriesTopics: the topics in a start request must reach the
// actor. Without this an actor started from the dashboard subscribes to the
// default namespace and never receives the subject the operator expected.
func TestSchedulerStartCarriesTopics(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	var got []string
	reglet := &topicsReglet{reg: reg, got: &got}
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	body := `{"plugin":"echo","topics":["worker.>"]}`
	if err := reg.Publish("supervision.start.worker-9", []byte(body)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return len(reglet.readGot()) > 0 }) {
		t.Fatal("actor never started")
	}
	gotTopics := reglet.readGot()
	if len(gotTopics) != 1 || gotTopics[0] != "worker.>" {
		t.Fatalf("topics were not carried to the start call: %v", gotTopics)
	}
}

// topicsReglet records the topics the scheduler passes to StartActor.
type topicsReglet struct {
	reg *Registry
	got *[]string

	mu   sync.Mutex
	once bool
}

func (r *topicsReglet) Registry() *Registry { return r.reg }
func (r *topicsReglet) StartActor(id, plugin string, topics []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.once {
		*r.got = append(*r.got, topics...)
		r.once = true
	}
	return nil
}

func (r *topicsReglet) readGot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), *r.got...)
}
func (r *topicsReglet) StopActor(string) error { return nil }
func (r *topicsReglet) Running() []string      { return nil }

// TestSchedulerStartWithoutPluginDefaults: a bare publish must still work, so
// the simple dashboard flow does not need a JSON body.
func TestSchedulerStartWithoutPluginDefaults(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reglet := newSchedulerReglet(reg)
	sched := NewSchedulerActor("sched", reglet, NewLeastLoadedPlacement(), "node-a")
	if err := reg.Start(sched); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}

	if err := reg.Publish("supervision.start.ui-2", nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return reglet.startCount("ui-2") == 1 }) {
		t.Fatal("bare supervision.start publish did not start an actor")
	}
}

// TestSchedulerIgnoresItsOwnPlacementSubject guards the loop: the scheduler
// subscribes to scheduler.plan.> and publishes scheduler.applied.>, which must
// not re-enter the reconcile path.
func TestSchedulerTopicsDoNotOverlap(t *testing.T) {
	sched := NewSchedulerActor("sched", nil, nil, "node-a")
	for _, topic := range sched.Topics() {
		if topic == "scheduler.applied.>" || topic == "scheduler.error.>" {
			t.Fatalf("scheduler subscribes to its own output topic %q", topic)
		}
	}
	_ = context.Background()
}

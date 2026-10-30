package kernel

import (
	"context"
	"testing"
	"time"
)

// TestPlacementPicksLeastLoaded verifies the simplest useful policy: spread
// actors across nodes rather than piling them onto one.
func TestPlacementPicksLeastLoaded(t *testing.T) {
	p := NewLeastLoadedPlacement()
	ctx := context.Background()

	// Empty cluster: deterministic tie-break (sorted node name).
	node, err := p.Place(ctx, PlacementRequest{Actor: "a"},
		newPlacementView(map[string]int{"only-node": 0}, nil))
	if err != nil {
		t.Fatalf("place on single node: %v", err)
	}
	if node != "only-node" {
		t.Fatalf("expected only-node, got %q", node)
	}

	// With one node holding two actors and another holding none, the empty one
	// wins.
	load := map[string]int{"node-a": 2, "node-b": 0}
	node, err = p.Place(ctx, PlacementRequest{Actor: "b"}, newPlacementView(load, nil))
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if node != "node-b" {
		t.Fatalf("expected node-b (least loaded), got %q", node)
	}
}

// TestPlacementRespectsLabels verifies actors can require a capable node —
// the minimum needed to express "this actor needs a GPU/Docker/whatever".
func TestPlacementRespectsLabels(t *testing.T) {
	p := NewLeastLoadedPlacement()
	ctx := context.Background()

	labels := map[string]map[string]string{
		"plain-1": {"zone": "a"},
		"gpu-1":   {"zone": "a", "gpu": "true"},
	}
	load := map[string]int{"plain-1": 5, "gpu-1": 5}

	node, err := p.Place(ctx, PlacementRequest{
		Actor:    "trainer",
		Requires: map[string]string{"gpu": "true"},
	}, loadWithLabels(load, labels))
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if node != "gpu-1" {
		t.Fatalf("expected gpu-1 (only node with gpu=true), got %q", node)
	}

	if _, err := p.Place(ctx, PlacementRequest{
		Actor:    "impossible",
		Requires: map[string]string{"tpu": "true"},
	}, loadWithLabels(load, labels)); err == nil {
		t.Fatal("expected placement failure when no node satisfies the constraints")
	}
}

// TestPlacementPinnedActor: a request can pin to a specific node, which is how
// an operator overrides the policy.
func TestPlacementPinnedActor(t *testing.T) {
	p := NewLeastLoadedPlacement()
	ctx := context.Background()
	node, err := p.Place(ctx, PlacementRequest{Actor: "x", Pin: "node-z"},
		newPlacementView(map[string]int{"node-a": 0, "node-z": 9}, nil))
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if node != "node-z" {
		t.Fatalf("pin ignored: got %q", node)
	}
}

// TestPlacementIsDeterministic: same inputs, same answer. A placement decision
// that wobbles between identical calls makes failover unpredictable.
func TestPlacementIsDeterministic(t *testing.T) {
	p := NewLeastLoadedPlacement()
	ctx := context.Background()
	view := newPlacementView(map[string]int{"node-a": 1, "node-b": 1, "node-c": 1}, nil)
	first, err := p.Place(ctx, PlacementRequest{Actor: "same"}, view)
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	for i := 0; i < 20; i++ {
		got, err := p.Place(ctx, PlacementRequest{Actor: "same"}, view)
		if err != nil {
			t.Fatalf("place %d: %v", i, err)
		}
		if got != first {
			t.Fatalf("non-deterministic placement: %q then %q", first, got)
		}
	}
}

// TestPlacementFailsWithNoNodes: an explicit error rather than a silent empty
// string, so callers cannot accidentally "place" onto nowhere.
func TestPlacementFailsWithNoNodes(t *testing.T) {
	p := NewLeastLoadedPlacement()
	if _, err := p.Place(context.Background(), PlacementRequest{Actor: "x"}, newPlacementView(nil, nil)); err == nil {
		t.Fatal("expected an error when no nodes are available")
	}
}

// TestSupervisorBackoffAndLimit verifies restart intensity is bounded: a
// crash-looping actor must not be restarted forever at full speed.
func TestSupervisorBackoffAndLimit(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	sup := NewSupervisorActor("sup", reg)
	sup.MaxRestarts = 3
	sup.RestartWindow = 5 * time.Second

	a := &failingActor{id: "crashloop", topics: []string{"crash.>"}}
	sup.Watch(a)

	if err := reg.Start(sup); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start actor: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := reg.Publish("supervision.failure.crashloop", []byte("boom")); err != nil {
			t.Fatalf("publish failure %d: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// After exceeding the limit the supervisor gives up and reports it.
	sup.mu.Lock()
	attempts := sup.attempts["crashloop"]
	sup.mu.Unlock()
	if attempts > sup.MaxRestarts {
		t.Fatalf("supervisor exceeded restart limit: %d attempts (limit %d)", attempts, sup.MaxRestarts)
	}
}

// failingActor never handles anything; used to drive the supervisor.
type failingActor struct {
	id     string
	topics []string
}

func (a *failingActor) ID() string                      { return a.id }
func (a *failingActor) Topics() []string                { return a.topics }
func (a *failingActor) Handle(_ context.Context, _ Msg) {}

// loadWithLabels builds a placement view from separate load and label maps.
func loadWithLabels(load map[string]int, labels map[string]map[string]string) PlacementView {
	return newPlacementView(load, labels)
}

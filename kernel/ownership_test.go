package kernel

import (
	"context"
	"testing"
	"time"
)

// TestOwnedStartClaimsLease: claiming an actor on a registry makes the registry
// the single owner and holds a renewing lease.
func TestOwnedStartClaimsLease(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-start")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	reg := NewRegistry(nc)
	defer reg.Close()
	reg.WithOwnership(leases, "node-a")

	a := &recordingActor{id: "job-1", topics: []string{"jobs.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer reg.Stop(a)

	owner, ok, err := leases.Owner(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if !ok || owner.Node != "node-a" {
		t.Fatalf("expected node-a to own job-1, got %+v ok=%v", owner, ok)
	}
}

// TestSecondRegistryCannotOwnSameActor is the point of the whole exercise: one
// actor id, one live owner cluster-wide.
func TestSecondRegistryCannotOwnSameActor(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-dup")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	regA := NewRegistry(nc)
	defer regA.Close()
	regA.WithOwnership(leases, "node-a")

	regB := NewRegistry(nc)
	defer regB.Close()
	regB.WithOwnership(leases, "node-b")

	a := &recordingActor{id: "singleton", topics: []string{"singleton.>"}}
	if err := regA.Start(a); err != nil {
		t.Fatalf("start on node-a: %v", err)
	}
	defer regA.Stop(a)

	// The same actor id on another node must be refused, not silently
	// duplicated onto the bus.
	dup := &recordingActor{id: "singleton", topics: []string{"singleton.>"}}
	err = regB.Start(dup)
	if err == nil {
		t.Fatal("second registry started an actor whose lease is held elsewhere")
	}
	if _, ok := err.(*OwnershipError); !ok {
		t.Fatalf("expected *OwnershipError, got %T: %v", err, err)
	}
}

// TestOwnedStopReleasesLease: stopping the actor frees the lease so another
// node can take over immediately.
func TestOwnedStopReleasesLease(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-release")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	regA := NewRegistry(nc)
	defer regA.Close()
	regA.WithOwnership(leases, "node-a")

	regB := NewRegistry(nc)
	defer regB.Close()
	regB.WithOwnership(leases, "node-b")

	a := &recordingActor{id: "movable", topics: []string{"movable.>"}}
	if err := regA.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := regA.Stop(a); err != nil {
		t.Fatalf("stop: %v", err)
	}

	b := &recordingActor{id: "movable", topics: []string{"movable.>"}}
	if err := regB.Start(b); err != nil {
		t.Fatalf("node-b could not take over after release: %v", err)
	}
	defer regB.Stop(b)
}

// TestOwnedStartWithoutJetStreamFails: ownership requires a durable lease, so a
// registry configured for ownership on a plain connection must say so rather
// than silently running without it.
func TestOwnedStartWithoutJetStreamFails(t *testing.T) {
	// embeddedBus has JetStream, so exercise the failure by closing the lease
	// store instead: Start must not proceed without a working lease.
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-closed")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	reg := NewRegistry(nc)
	defer reg.Close()
	reg.WithOwnership(leases, "node-a")

	if err := leases.Close(); err != nil {
		t.Fatalf("close leases: %v", err)
	}

	err = reg.Start(&recordingActor{id: "unownable", topics: []string{"x.>"}})
	if err == nil {
		t.Fatal("Start succeeded without a working lease store")
	}
	if _, ok := err.(*OwnershipError); !ok {
		t.Fatalf("expected *OwnershipError, got %T: %v", err, err)
	}
}

// TestLostLeaseStopsTheActor is the safety property behind the lease. Detecting
// that another node has taken over is useless unless the local actor actually
// stops: continuing to consume would mean two live owners, which is the exact
// failure the lease exists to prevent.
func TestLostLeaseStopsTheActor(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-revoke")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	leases.TTL = 300 * time.Millisecond

	reg := NewRegistry(nc)
	defer reg.Close()
	reg.WithOwnership(leases, "node-a")

	a := &recordingActor{id: "revocable", topics: []string{"revocable.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Sanity: it is running and owning.
	if !waitFor(t, time.Second, func() bool {
		owner, ok, _ := leases.Owner(context.Background(), "revocable")
		return ok && owner.Node == "node-a"
	}) {
		t.Fatal("actor never took ownership")
	}

	// Simulate a takeover by writing a lease for another node directly. The
	// renew loop should notice and revoke the local actor.
	ctx := context.Background()
	holder, ok, err := leases.Owner(ctx, "revocable")
	if err != nil || !ok {
		t.Fatalf("owner: ok=%v err=%v", ok, err)
	}
	if _, err := leases.kv.Update(ctx, "revocable",
		leases.encode("revocable", "node-b", time.Now(), time.Now()), holder.Revision); err != nil {
		t.Fatalf("simulate takeover: %v", err)
	}

	// The actor must stop being subscribed.
	if !waitFor(t, 3*time.Second, func() bool {
		reg.mu.Lock()
		_, present := reg.actorSub["revocable"]
		reg.mu.Unlock()
		return !present
	}) {
		t.Fatal("actor kept its subscription after losing ownership (two owners on the bus)")
	}
}

// TestFailedSubscribeReleasesLease: a Start that claims the lease but fails to
// subscribe must not leave the lease held, or the actor becomes unstartable
// everywhere.
func TestFailedSubscribeReleasesLease(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-leak")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	reg := NewRegistry(nc)
	defer reg.Close()
	reg.WithOwnership(leases, "node-a")

	// An invalid subject makes nc.Subscribe fail after the claim succeeds.
	bad := &recordingActor{id: "unsubscribable", topics: []string{"bad subject with spaces"}}
	if err := reg.Start(bad); err == nil {
		t.Fatal("expected subscribe to fail for an invalid subject")
	}

	if _, ok, err := leases.Owner(context.Background(), "unsubscribable"); err != nil || ok {
		t.Fatalf("lease leaked after failed Start: ok=%v err=%v", ok, err)
	}
}

// TestLeaseRenewedWhileRunning verifies the ownership heartbeat keeps the lease
// alive past its TTL, so a healthy node never loses an actor to its own clock.
func TestLeaseRenewedWhileRunning(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	leases, err := NewLeaseStore(nc, "owned-heartbeat")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	leases.TTL = 200 * time.Millisecond

	reg := NewRegistry(nc)
	defer reg.Close()
	reg.WithOwnership(leases, "node-a")

	a := &recordingActor{id: "longrunner", topics: []string{"long.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer reg.Stop(a)

	// Hold well past the TTL; the renew loop must keep ownership.
	deadline := time.Now().Add(3 * leases.TTL)
	for time.Now().Before(deadline) {
		owner, ok, err := leases.Owner(context.Background(), "longrunner")
		if err != nil {
			t.Fatalf("owner: %v", err)
		}
		if !ok || owner.Node != "node-a" {
			t.Fatalf("lost ownership while running: %+v ok=%v", owner, ok)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestLeaseSingleOwner: exactly one owner wins a contested claim.
func TestLeaseSingleOwner(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	a, err := NewLeaseStore(nc, "owner-test")
	if err != nil {
		t.Fatalf("lease store a: %v", err)
	}
	b, err := NewLeaseStore(nc, "owner-test")
	if err != nil {
		t.Fatalf("lease store b: %v", err)
	}

	won, err := a.Claim(ctx, "actor-1", "node-a")
	if err != nil {
		t.Fatalf("claim a: %v", err)
	}
	if !won {
		t.Fatal("first claim should win")
	}
	won, err = b.Claim(ctx, "actor-1", "node-b")
	if err != nil {
		t.Fatalf("claim b: %v", err)
	}
	if won {
		t.Fatal("second claim on a held lease must lose")
	}

	owner, ok, err := a.Owner(ctx, "actor-1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if !ok || owner.Node != "node-a" {
		t.Fatalf("expected node-a to own the lease, got %+v ok=%v", owner, ok)
	}
}

// TestLeaseClaimIsIdempotentForTheSameNode: a node re-claiming its own lease
// succeeds (restart of the same actor on the same node).
func TestLeaseClaimIsIdempotentForTheSameNode(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewLeaseStore(nc, "owner-idem")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	for i := 0; i < 3; i++ {
		won, err := store.Claim(ctx, "actor-1", "node-a")
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if !won {
			t.Fatalf("re-claim %d by the owning node should win", i)
		}
	}
}

// TestLeaseRenewExtends: renewal keeps ownership and extends the expiry.
func TestLeaseRenewExtends(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewLeaseStore(nc, "owner-renew")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	if _, err := store.Claim(ctx, "actor-1", "node-a"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	before, _, err := store.Owner(ctx, "actor-1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	held, err := store.Renew(ctx, "actor-1", "node-a")
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !held {
		t.Fatal("owner should be able to renew its own lease")
	}
	after, _, err := store.Owner(ctx, "actor-1")
	if err != nil {
		t.Fatalf("owner after renew: %v", err)
	}
	if !after.ExpiresAt.After(before.ExpiresAt) {
		t.Fatalf("renew should extend expiry: before=%v after=%v", before.ExpiresAt, after.ExpiresAt)
	}

	// A non-owner must not be able to renew.
	held, err = store.Renew(ctx, "actor-1", "node-b")
	if err != nil {
		t.Fatalf("renew by non-owner: %v", err)
	}
	if held {
		t.Fatal("non-owner must not renew")
	}
}

// TestLeaseExpiresAndCanBeStolen: an expired lease can be taken over by another
// node — this is what makes a crashed owner recoverable.
func TestLeaseExpiresAndCanBeStolen(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewLeaseStore(nc, "owner-expiry")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	store.TTL = 50 * time.Millisecond

	if _, err := store.Claim(ctx, "actor-1", "node-a"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// While held, another node loses the race.
	won, err := store.Claim(ctx, "actor-1", "node-b")
	if err != nil {
		t.Fatalf("claim while held: %v", err)
	}
	if won {
		t.Fatal("claim while lease is live must lose")
	}

	// After expiry, the next claim wins.
	if !waitFor(t, 2*time.Second, func() bool {
		won, err := store.Claim(ctx, "actor-1", "node-b")
		return err == nil && won
	}) {
		t.Fatal("expired lease was never stolen")
	}

	owner, ok, err := store.Owner(ctx, "actor-1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if !ok || owner.Node != "node-b" {
		t.Fatalf("expected node-b to have taken over, got %+v ok=%v", owner, ok)
	}
}

// TestLeaseRelease: the owner can release early, letting another node claim
// immediately rather than waiting for expiry.
func TestLeaseRelease(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewLeaseStore(nc, "owner-release")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	if _, err := store.Claim(ctx, "actor-1", "node-a"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Release(ctx, "actor-1", "node-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok, err := store.Owner(ctx, "actor-1"); err != nil || ok {
		t.Fatalf("expected no owner after release, got ok=%v err=%v", ok, err)
	}

	won, err := store.Claim(ctx, "actor-1", "node-b")
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if !won {
		t.Fatal("claim after release should win")
	}
}

// TestLeaseReleaseByNonOwnerIsNoop: releasing someone else's lease must not
// free it.
func TestLeaseReleaseByNonOwnerIsNoop(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewLeaseStore(nc, "owner-release-guard")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	if _, err := store.Claim(ctx, "actor-1", "node-a"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Release(ctx, "actor-1", "node-b"); err != nil {
		t.Fatalf("release by non-owner: %v", err)
	}
	if owner, ok, err := store.Owner(ctx, "actor-1"); err != nil || !ok || owner.Node != "node-a" {
		t.Fatalf("lease was freed by a non-owner: %+v ok=%v err=%v", owner, ok, err)
	}
}

// TestLeaseConcurrentClaimsHaveOneWinner is the real test of the lease: many
// racing claimants, exactly one winner.
func TestLeaseConcurrentClaimsHaveOneWinner(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	ctx := context.Background()
	store, err := NewLeaseStore(nc, "owner-race")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}

	const claimants = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners []string
	)
	start := make(chan struct{})
	for i := 0; i < claimants; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			node := "node-" + string(rune('a'+n))
			won, err := store.Claim(ctx, "contested", node)
			if err != nil {
				t.Errorf("claim %s: %v", node, err)
				return
			}
			if won {
				mu.Lock()
				winners = append(winners, node)
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("expected exactly 1 winner among %d claimants, got %d: %v", claimants, len(winners), winners)
	}
	owner, ok, err := store.Owner(ctx, "contested")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if !ok || owner.Node != winners[0] {
		t.Fatalf("owner %+v does not match winner %q", owner, winners[0])
	}
}

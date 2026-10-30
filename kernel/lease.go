// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"
)

// Ownership is what gives an actor id exactly one live owner across a cluster.
//
// The kernel otherwise has no actor references: an actor is a local Go value
// subscribed to a subject, so two nodes starting "job-1" both subscribe and
// both receive every message. A LeaseStore fixes that by making the actor id a
// key in a JetStream KV bucket. Claiming the key is a compare-and-set, so at
// most one node can hold it. The holder renews on a heartbeat; if it dies the
// lease expires and another node can take over.
//
// This is deliberately not a placement engine. It does not decide where an
// actor should run, and it does not move state between nodes. It answers one
// question — "may I run this actor here, now?" — which is the minimum needed
// for an actor id to mean one thing cluster-wide.

// Lease is the record held under an actor's key in the ownership bucket.
type Lease struct {
	// Actor is the actor id this lease covers.
	Actor string `json:"actor"`
	// Node identifies the owner. Two registries on the same node should pass
	// the same value; ownership is per (actor, node) pair, not per process.
	Node string `json:"node"`
	// ExpiresAt is when the lease becomes claimable by another node. The owner
	// pushes it forward on every renew.
	ExpiresAt time.Time `json:"expires_at"`
	// AcquiredAt is when this node first took the lease. Kept across renewals
	// so a takeover is distinguishable from a fresh start.
	AcquiredAt time.Time `json:"acquired_at"`
	// Revision is the JetStream KV revision this lease was written at. Used to
	// release exactly this lease and not a later one held by someone else.
	Revision uint64 `json:"-"`
}

// Held reports whether the lease is still live at time now.
func (l Lease) Held(now time.Time) bool {
	return now.Before(l.ExpiresAt)
}

// LeaseStore manages ownership leases for actor ids in a JetStream KV bucket.
// All methods are safe for concurrent use.
type LeaseStore struct {
	kv     js.KeyValue
	bucket string

	// TTL is how long a claimed lease stays valid without renewal. The owning
	// registry renews at TTL/3, so a missed renew only costs a third of the TTL
	// before ownership is at risk.
	TTL time.Duration

	mu     sync.Mutex
	closed bool
}

// DefaultLeaseTTL is the lease lifetime used when TTL is left unset.
const DefaultLeaseTTL = 15 * time.Second

// ownershipBucket is the default KV bucket name for ownership leases.
const ownershipBucket = "actor_ownership"

// NewLeaseStore opens (or creates) the ownership lease bucket. Pass an empty
// bucket name for the default ("actor_ownership").
//
// The bucket keeps key history so a takeover can be observed after the fact,
// but expiry is enforced by the lease record itself rather than by KV TTLs:
// a per-key TTL would delete the key outright, losing the record of who owned
// it, and would need LimitMarkerTTL to be useful.
func NewLeaseStore(nc *nats.Conn, bucket string) (*LeaseStore, error) {
	if bucket == "" {
		bucket = ownershipBucket
	}
	jsc, err := js.New(nc)
	if err != nil {
		return nil, fmt.Errorf("lease: new jetstream context: %w", err)
	}
	ctx := context.Background()
	kv, err := jsc.CreateKeyValue(ctx, js.KeyValueConfig{
		Bucket:      bucket,
		Description: "actor ownership leases",
		History:     4, // enough to see a takeover, small enough to stay cheap
	})
	if err != nil {
		// The bucket already exists; open it instead. This is the normal path
		// on every start after the first.
		kv, err = jsc.KeyValue(ctx, bucket)
		if err != nil {
			return nil, fmt.Errorf("lease: create or open bucket %q: %w", bucket, err)
		}
	}
	return &LeaseStore{kv: kv, bucket: bucket, TTL: DefaultLeaseTTL}, nil
}

// Bucket returns the KV bucket name backing the lease store.
func (l *LeaseStore) Bucket() string { return l.bucket }

// Claim attempts to take ownership of actor for node. It returns true when the
// node holds the lease afterwards, whether it just won it or already held it.
//
// The claim is a compare-and-set on the KV key: an absent key is created (Create
// refuses if it already exists), a held key is replaced only if its expiry has
// passed (Update with the stored revision). Two nodes racing produce one winner
// because JetStream serialises both operations on the same key.
func (l *LeaseStore) Claim(ctx context.Context, actor, node string) (bool, error) {
	if actor == "" || node == "" {
		return false, fmt.Errorf("lease: claim needs both actor and node")
	}
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return false, fmt.Errorf("lease: store closed")
	}

	for attempt := 0; attempt < 3; attempt++ {
		now := time.Now()
		holder, ok, err := l.Owner(ctx, actor)
		if err != nil {
			return false, err
		}

		switch {
		case !ok:
			// No live record: try to create one. If another node created it
			// first, Create fails and we loop to re-read.
			rev, err := l.kv.Create(ctx, actor, l.encode(actor, node, now, now))
			if err == nil {
				_ = rev
				return true, nil
			}
			if errors.Is(err, js.ErrKeyExists) {
				continue // lost the race; re-read and re-evaluate
			}
			return false, fmt.Errorf("lease: claim %q: %w", actor, err)

		case holder.Node == node:
			// Already ours: extend rather than fight our own record. A restart
			// of the same actor on the same node lands here.
			rev, err := l.kv.Update(ctx, actor, l.encode(actor, node, holder.AcquiredAt, now), holder.Revision)
			if err == nil {
				_ = rev
				return true, nil
			}
			if errors.Is(err, js.ErrKeyRevisionMismatch) {
				continue // someone else touched it; re-read
			}
			return false, fmt.Errorf("lease: re-claim %q: %w", actor, err)

		case !holder.Held(now):
			// Expired: take it over. Update with the revision we read, so a
			// concurrent takeover by a third node makes exactly one of us lose.
			rev, err := l.kv.Update(ctx, actor, l.encode(actor, node, now, now), holder.Revision)
			if err == nil {
				_ = rev
				return true, nil
			}
			if errors.Is(err, js.ErrKeyRevisionMismatch) {
				continue
			}
			return false, fmt.Errorf("lease: take over %q: %w", actor, err)

		default:
			// Live lease held by another node.
			return false, nil
		}
	}
	return false, fmt.Errorf("lease: claim %q: gave up after repeated write conflicts", actor)
}

// Renew extends a lease the caller already holds. It returns false (and no
// error) when the caller is not the owner or the lease was taken over.
func (l *LeaseStore) Renew(ctx context.Context, actor, node string) (bool, error) {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return false, fmt.Errorf("lease: store closed")
	}

	holder, ok, err := l.Owner(ctx, actor)
	if err != nil {
		return false, err
	}
	if !ok || holder.Node != node {
		return false, nil
	}
	_, err = l.kv.Update(ctx, actor, l.encode(actor, node, holder.AcquiredAt, time.Now()), holder.Revision)
	if err != nil {
		if errors.Is(err, js.ErrKeyRevisionMismatch) {
			return false, nil // lost a race with a takeover
		}
		return false, fmt.Errorf("lease: renew %q: %w", actor, err)
	}
	return true, nil
}

// Release drops the lease if node is its owner. Releasing a lease held by
// another node is a no-op, not an error: a node should always be able to say
// "stop tracking this" without being able to steal it.
func (l *LeaseStore) Release(ctx context.Context, actor, node string) error {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return nil // nothing to release once closed
	}

	holder, ok, err := l.Owner(ctx, actor)
	if err != nil {
		return err
	}
	if !ok || holder.Node != node {
		return nil
	}
	if err := l.kv.Delete(ctx, actor, js.LastRevision(holder.Revision)); err != nil {
		if errors.Is(err, js.ErrKeyRevisionMismatch) || errors.Is(err, js.ErrKeyNotFound) {
			return nil // already gone or already replaced
		}
		return fmt.Errorf("lease: release %q: %w", actor, err)
	}
	return nil
}

// Owner returns the current lease for actor. The second return value is false
// when no record exists at all (never claimed, or released).
//
// An expired lease is still returned, with ok=true; callers decide whether an
// expired holder matters. Held() answers that.
func (l *LeaseStore) Owner(ctx context.Context, actor string) (Lease, bool, error) {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return Lease{}, false, fmt.Errorf("lease: store closed")
	}

	// A released key keeps a delete marker; KeyValue.Get reports that as
	// ErrKeyNotFound, so one case covers "never claimed" and "released".
	entry, err := l.kv.Get(ctx, actor)
	if err != nil {
		if errors.Is(err, js.ErrKeyNotFound) {
			return Lease{}, false, nil
		}
		return Lease{}, false, fmt.Errorf("lease: read %q: %w", actor, err)
	}

	var lease Lease
	if err := json.Unmarshal(entry.Value(), &lease); err != nil {
		return Lease{}, false, fmt.Errorf("lease: decode %q: %w", actor, err)
	}
	lease.Revision = entry.Revision()
	return lease, true, nil
}

// List returns every lease currently recorded. Expired leases are included;
// use Held to filter.
func (l *LeaseStore) List(ctx context.Context) ([]Lease, error) {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("lease: store closed")
	}

	keys, err := l.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, js.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("lease: list keys: %w", err)
	}
	out := make([]Lease, 0, len(keys))
	for _, k := range keys {
		lease, ok, err := l.Owner(ctx, k)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, lease)
		}
	}
	return out, nil
}

// Close releases the store handle. It is idempotent.
func (l *LeaseStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

// encode builds the lease value written to the bucket. acquiredAt is preserved
// across renewals so a takeover can be told apart from a fresh start.
func (l *LeaseStore) encode(actor, node string, acquiredAt, now time.Time) []byte {
	lease := Lease{
		Actor:      actor,
		Node:       node,
		AcquiredAt: acquiredAt,
		ExpiresAt:  now.Add(l.ttl()),
	}
	data, _ := json.Marshal(lease)
	return data
}

// ttl returns the configured TTL, falling back to the default when unset.
func (l *LeaseStore) ttl() time.Duration {
	if l.TTL > 0 {
		return l.TTL
	}
	return DefaultLeaseTTL
}

// OwnershipError reports that an actor could not be started because its lease
// is held elsewhere (or could not be established at all).
type OwnershipError struct {
	Actor string
	Node  string
	Owner string // node currently holding the lease, empty if unknown
	Err   error
}

// Error implements error.
func (e *OwnershipError) Error() string {
	if e.Owner != "" {
		return fmt.Sprintf("actor %q: ownership held by node %q (this node: %q)", e.Actor, e.Owner, e.Node)
	}
	if e.Err != nil {
		return fmt.Sprintf("actor %q: ownership lease unavailable: %v", e.Actor, e.Err)
	}
	return fmt.Sprintf("actor %q: ownership lease unavailable (this node: %q)", e.Actor, e.Node)
}

// Unwrap exposes the underlying cause for errors.Is/As.
func (e *OwnershipError) Unwrap() error { return e.Err }

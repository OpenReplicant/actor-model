// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"
)

// KVStore is a durable key-value store actors can opt into for snapshotting
// state across restarts. It is optional and not on by default — actors using
// it own their key schema and are responsible for reading/writing their own
// state.
type KVStore interface {
	// Get returns the value stored at key, or nil if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put stores value at key.
	Put(ctx context.Context, key string, value []byte) error
	// Delete removes key. It is not an error to delete a missing key.
	Delete(ctx context.Context, key string) error
	// Close releases the store's resources.
	Close() error
}

// natsKVStore wraps a JetStream KV bucket.
type natsKVStore struct {
	kv     js.KeyValue
	mu     sync.Mutex
	closed bool
}

// NewKVStore creates (or opens) a JetStream KV bucket named bucket on the
// connection's JetStream account. If JetStream is not enabled on the server,
// an error is returned. Actors pass the store into themselves and own their
// key schema.
func NewKVStore(nc *nats.Conn, bucket string) (KVStore, error) {
	jsc, err := js.New(nc)
	if err != nil {
		return nil, fmt.Errorf("new jetstream context: %w", err)
	}
	kv, err := jsc.CreateKeyValue(context.Background(), js.KeyValueConfig{
		Bucket: bucket,
	})
	if err != nil {
		// Bucket may already exist; fetch it instead.
		if kv, err = jsc.KeyValue(context.Background(), bucket); err != nil {
			return nil, fmt.Errorf("create or fetch kv bucket %q: %w", bucket, err)
		}
	}
	return &natsKVStore{kv: kv}, nil
}

// Get returns the value stored at key, or nil if the key does not exist.
func (s *natsKVStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("kv store closed")
	}
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, js.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return entry.Value(), nil
}

// Put stores value at key.
func (s *natsKVStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("kv store closed")
	}
	_, err := s.kv.Put(ctx, key, value)
	return err
}

// Delete removes key. It is not an error to delete a missing key.
func (s *natsKVStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("kv store closed")
	}
	if err := s.kv.Delete(ctx, key); err != nil {
		if errors.Is(err, js.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// Close releases the store's resources. It is idempotent.
func (s *natsKVStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

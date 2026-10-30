package kernel

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// jsBus starts an in-process NATS server with JetStream enabled and returns a
// *nats.Conn plus a cleanup function.
func jsBus(t *testing.T) (*nats.Conn, func()) {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		// Isolated store. Sharing NATS's default temp-backed store leaks KV
		// buckets (and leases) between tests and between runs, which surfaces as
		// "already owned" or "key exists" failures that depend on run order.
		StoreDir: t.TempDir(),
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats server not ready")
	}

	nc, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("connect to nats: %v", err)
	}

	return nc, func() {
		nc.Close()
		srv.Shutdown()
	}
}

func TestKVStorePutGetDelete(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	store, err := NewKVStore(nc, "test-bucket")
	if err != nil {
		t.Fatalf("new kv store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Missing key returns nil value, no error.
	val, err := store.Get(ctx, "missing")
	if err != nil {
		t.Fatalf("get missing key: %v", err)
	}
	if val != nil {
		t.Fatalf("expected nil for missing key, got %q", val)
	}

	// Put then Get.
	if err := store.Put(ctx, "foo", []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err = store.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("get foo: %v", err)
	}
	if string(val) != "bar" {
		t.Fatalf("expected 'bar', got %q", val)
	}

	// Overwrite.
	if err := store.Put(ctx, "foo", []byte("baz")); err != nil {
		t.Fatalf("put overwrite: %v", err)
	}
	val, err = store.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("get foo after overwrite: %v", err)
	}
	if string(val) != "baz" {
		t.Fatalf("expected 'baz', got %q", val)
	}

	// Delete then Get returns nil.
	if err := store.Delete(ctx, "foo"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	val, err = store.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if val != nil {
		t.Fatalf("expected nil after delete, got %q", val)
	}

	// Delete of missing key is not an error.
	if err := store.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
}

func TestKVStoreBucketReuse(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	store1, err := NewKVStore(nc, "reuse-bucket")
	if err != nil {
		t.Fatalf("new kv store 1: %v", err)
	}
	if err := store1.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("put on store1: %v", err)
	}
	store1.Close()

	// Re-opening the same bucket should preserve data.
	store2, err := NewKVStore(nc, "reuse-bucket")
	if err != nil {
		t.Fatalf("new kv store 2: %v", err)
	}
	defer store2.Close()

	val, err := store2.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("get on store2: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("expected 'v' persisted across buckets, got %q", val)
	}
}

func TestKVStoreCloseThenUse(t *testing.T) {
	nc, cleanup := jsBus(t)
	defer cleanup()

	store, err := NewKVStore(nc, "closed-bucket")
	if err != nil {
		t.Fatalf("new kv store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}

	ctx := context.Background()
	if _, err := store.Get(ctx, "k"); err == nil {
		t.Fatal("expected error on get after close")
	}
	if err := store.Put(ctx, "k", []byte("v")); err == nil {
		t.Fatal("expected error on put after close")
	}
	if err := store.Delete(ctx, "k"); err == nil {
		t.Fatal("expected error on delete after close")
	}
}

func waitForKV(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

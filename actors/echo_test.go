package actors

import (
	"context"
	"sync"
	"testing"
	"time"

	kernel "actor-kernel/kernel"
)

// captureActor wraps an inner actor so the test can observe handles while the
// inner actor does the real work (e.g. echoing replies).
type captureActor struct {
	id      string
	topics  []string
	reg     *kernel.Registry
	inner   kernel.Actor
	mu      sync.Mutex
	msgs    []kernel.Msg
	handles int
}

func (a *captureActor) ID() string       { return a.id }
func (a *captureActor) Topics() []string { return a.topics }
func (a *captureActor) Handle(ctx context.Context, msg kernel.Msg) {
	a.mu.Lock()
	a.msgs = append(a.msgs, msg)
	a.handles++
	a.mu.Unlock()
	a.inner.Handle(ctx, msg)
}
func (a *captureActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.handles
}

func TestEchoActorRepublishesOnReply(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	e := NewEchoActor("echo", []string{"echo.>"}, reg)
	c := &captureActor{id: "echo-capture", topics: []string{"echo.>"}, reg: reg, inner: e}
	if err := reg.Start(c); err != nil {
		t.Fatalf("start: %v", err)
	}

	reply, err := reg.Request("echo.test", []byte("hello"), 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(reply) != "hello" {
		t.Fatalf("expected echo of 'hello', got %q", string(reply))
	}
	if got := c.count(); got != 1 {
		t.Fatalf("expected 1 handle, got %d", got)
	}

	if err := reg.Publish("echo.no-reply", []byte("silent")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return c.count() == 2 }) {
		t.Fatalf("expected 2 handles, got %d", c.count())
	}
}

func TestEchoActorOnlyMatchesItsTopic(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	e := NewEchoActor("echo", []string{"echo.>"}, reg)
	c := &captureActor{id: "echo-capture", topics: []string{"echo.>"}, reg: reg, inner: e}
	if err := reg.Start(c); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := reg.Publish("other.topic", []byte("ignored")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := c.count(); got != 0 {
		t.Fatalf("expected 0 handles for non-matching topic, got %d", got)
	}
}

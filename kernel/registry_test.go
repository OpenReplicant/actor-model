package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// replyActor handles "echo.>" and replies on msg.Reply.
type replyActor struct {
	id       string
	topics   []string
	reg      *Registry
	handles  int
	lastSubj string
	lastData []byte
	mu       sync.Mutex
}

func (r *replyActor) ID() string       { return r.id }
func (r *replyActor) Topics() []string { return r.topics }
func (r *replyActor) Handle(ctx context.Context, msg Msg) {
	r.mu.Lock()
	r.handles++
	r.lastSubj = msg.Subject
	r.lastData = msg.Data
	r.mu.Unlock()
	if msg.Reply != "" {
		if err := r.reg.Publish(msg.Reply, []byte("pong")); err != nil {
			return
		}
	}
}
func (r *replyActor) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handles
}

func TestRegistryPublishDeliversToActor(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := &recordingActor{id: "temp", topics: []string{"sensors.temp.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := reg.Publish("sensors.temp.office", []byte(`{"value":23.5}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool { return a.count() == 1 }) {
		t.Fatalf("expected 1 handle, got %d; last subject=%q", a.count(), a.lastSubject())
	}
	if got := a.lastSubject(); got != "sensors.temp.office" {
		t.Fatalf("expected subject sensors.temp.office, got %q", got)
	}
	if got := string(a.lastData()); got != `{"value":23.5}` {
		t.Fatalf("expected payload, got %q", string(a.lastData()))
	}
}

func TestRegistryWildcardSubscription(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := &recordingActor{id: "wild", topics: []string{"sensors.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	for _, subj := range []string{"sensors.temp", "sensors.temp.office", "sensors.humidity"} {
		if err := reg.Publish(subj, []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	if !waitFor(t, 2*time.Second, func() bool { return a.count() >= 3 }) {
		t.Fatalf("expected >=3 handles, got %d", a.count())
	}
}

func TestRegistryStopUnsubscribes(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := &recordingActor{id: "temp", topics: []string{"sensors.temp.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := reg.Stop(a); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if err := reg.Publish("sensors.temp.office", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := a.count(); got != 0 {
		t.Fatalf("expected 0 handles after stop, got %d", got)
	}
}

func TestRegistryMultipleActorsShareTopic(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := &recordingActor{id: "a", topics: []string{"shared.>"}}
	b := &recordingActor{id: "b", topics: []string{"shared.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start a: %v", err)
	}
	if err := reg.Start(b); err != nil {
		t.Fatalf("start b: %v", err)
	}

	if err := reg.Publish("shared.msg", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return a.count() == 1 && b.count() == 1 }) {
		t.Fatalf("expected both actors to handle, got a=%d b=%d", a.count(), b.count())
	}

	// Stopping one actor must not affect the other.
	if err := reg.Stop(a); err != nil {
		t.Fatalf("stop a: %v", err)
	}
	if err := reg.Publish("shared.msg2", []byte("y")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := a.count(); got != 1 {
		t.Fatalf("expected a to stay at 1 after stop, got %d", got)
	}
	if !waitFor(t, 2*time.Second, func() bool { return b.count() == 2 }) {
		t.Fatalf("expected b to handle 2, got %d", b.count())
	}
}

func TestRegistryRequestReply(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	// replier handles "echo.>" and replies on msg.Reply.
	r := &replyActor{id: "replier", topics: []string{"echo.>"}, reg: reg}
	if err := reg.Start(r); err != nil {
		t.Fatalf("start replier: %v", err)
	}

	if err := reg.Publish("echo.request", []byte("ping")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return r.count() == 1 }) {
		t.Fatalf("expected replier to handle 1 message, got %d", r.count())
	}

	// Now do an actual request/reply through the registry. The replier replies
	// on msg.Reply, so reg.Request should receive "pong".
	reply, err := reg.Request("echo.request", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(reply) != "pong" {
		t.Fatalf("expected reply pong, got %q", string(reply))
	}
}

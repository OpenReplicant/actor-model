package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// panickingActor panics on the first message whose payload is "boom" and
// records every message it handles, so a test can tell whether delivery
// continued after the panic.
type panickingActor struct {
	id     string
	topics []string
	mu     sync.Mutex
	seen   []string
}

func (a *panickingActor) ID() string       { return a.id }
func (a *panickingActor) Topics() []string { return a.topics }

func (a *panickingActor) Handle(_ context.Context, msg Msg) {
	if string(msg.Data) == "boom" {
		panic("handler exploded")
	}
	a.mu.Lock()
	a.seen = append(a.seen, string(msg.Data))
	a.mu.Unlock()
}

func (a *panickingActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.seen)
}

// TestActorPanicDoesNotKillDelivery is the core guarantee: a handler that
// panics must not silently deafen the actor. Before recovery was added, the
// panic propagated out of the delivery goroutine, which exited permanently —
// the actor kept its subscription but never handled another message, and the
// supervisor never heard about it.
func TestActorPanicDoesNotKillDelivery(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := &panickingActor{id: "panicky", topics: []string{"panic.test"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := reg.Publish("panic.test", []byte("boom")); err != nil {
		t.Fatalf("publish boom: %v", err)
	}
	// Give the panic a chance to unwind before publishing the good message.
	time.Sleep(50 * time.Millisecond)

	if err := reg.Publish("panic.test", []byte("after")); err != nil {
		t.Fatalf("publish after: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool { return a.count() >= 1 }) {
		t.Fatal("actor stopped delivering after a handler panic (delivery goroutine died)")
	}
	if got := a.seen[0]; got != "after" {
		t.Fatalf("expected the post-panic message, got %q", got)
	}
}

// TestActorPanicIsReportedToSupervisor verifies a panicking handler publishes
// supervision.failure.<id>, so the existing supervisor can act on the failure
// it previously could never see.
func TestActorPanicIsReportedToSupervisor(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	cap := &recordingActor{id: "capture", topics: []string{"supervision.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start capture: %v", err)
	}

	a := &panickingActor{id: "reported", topics: []string{"panic.report"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := reg.Publish("panic.report", []byte("boom")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "supervision.failure.reported")
	}) {
		t.Fatalf("expected supervision.failure.reported; got %v", cap.subjects())
	}
}

// TestActorPanicOnJetStreamMessageIsAcked verifies the panic path still acks a
// JetStream message. If the ack were skipped the consumer would redeliver the
// poison message forever, turning one bad handler into an infinite loop.
func TestActorPanicOnJetStreamMessageIsAcked(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reg.StoreTopicConfigs("poison", []TopicConfig{
		{Subject: "poison.js", JetStream: true},
	})

	a := &panickingActor{id: "poison", topics: []string{"poison.js"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	js, err := reg.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.Publish("poison.js", []byte("boom")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Redelivery would show up as a second delivery of the same message; with
	// MaxDeliver this becomes a poison loop rather than an unbounded one.
	time.Sleep(300 * time.Millisecond)
	if err := reg.Publish("poison.js", []byte("after")); err != nil {
		t.Fatalf("publish after: %v", err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return a.count() >= 1 }) {
		t.Fatal("actor did not resume after panicking on a JetStream message")
	}

	// Ask the consumer directly: an acked panic message leaves nothing pending.
	si, err := js.StreamInfo("JS_poison_js")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if si.State.Msgs != 2 {
		t.Fatalf("expected 2 stored messages, got %d", si.State.Msgs)
	}
	ci, err := js.ConsumerInfo("JS_poison_js", sanitize("poison"))
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if ci.NumAckPending != 0 {
		t.Fatalf("panicked message left unacked (%d pending); it would redeliver forever", ci.NumAckPending)
	}
}

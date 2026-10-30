package kernel

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// countingActor counts how many times each message id reached Handle.
type countingActor struct {
	id     string
	topics []string
	mu     sync.Mutex
	seen   map[string]int
}

func newCountingActor(id string, topics []string) *countingActor {
	return &countingActor{id: id, topics: topics, seen: make(map[string]int)}
}

func (a *countingActor) ID() string       { return a.id }
func (a *countingActor) Topics() []string { return a.topics }
func (a *countingActor) Handle(_ context.Context, msg Msg) {
	var env struct {
		ID   string `json:"id"`
		Data string `json:"data"`
	}
	_ = json.Unmarshal(msg.Data, &env)
	a.mu.Lock()
	a.seen[env.ID]++
	a.mu.Unlock()
}
func (a *countingActor) count(id string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.seen[id]
}

// TestDedupSuppressesDuplicateDelivery: a JetStream consumer is at-least-once,
// so the same message can be delivered twice. With dedup enabled the handler
// sees it once.
func TestDedupSuppressesDuplicateDelivery(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reg.StoreTopicConfigs("dedup-actor", []TopicConfig{
		{Subject: "dedup.test", JetStream: true, Dedup: true},
	})

	a := newCountingActor("dedup-actor", []string{"dedup.test"})
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	body, _ := json.Marshal(map[string]string{"id": "msg-1", "data": "x"})
	// Publish the identical message three times — the same envelope id.
	for i := 0; i < 3; i++ {
		if err := reg.Publish("dedup.test", body); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	if !waitFor(t, 2*time.Second, func() bool { return a.count("msg-1") >= 1 }) {
		t.Fatal("message never handled")
	}
	time.Sleep(200 * time.Millisecond) // allow duplicates to arrive if not suppressed
	if got := a.count("msg-1"); got != 1 {
		t.Fatalf("expected exactly 1 handle, got %d (dedup did not suppress)", got)
	}
}

// TestDedupDisabledHandlesEveryCopy: the default must not silently change
// semantics for actors that rely on seeing every message.
func TestDedupDisabledHandlesEveryCopy(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	// Without StoreTopicConfigs requesting dedup, nothing is suppressed.
	a := newCountingActor("nodedup", []string{"nodedup.test"})
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}
	body, _ := json.Marshal(map[string]string{"id": "same", "data": "x"})
	for i := 0; i < 3; i++ {
		if err := reg.Publish("nodedup.test", body); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if !waitFor(t, 2*time.Second, func() bool { return a.count("same") == 3 }) {
		t.Fatalf("expected 3 handles without dedup, got %d", a.count("same"))
	}
}

// TestDedupWindowExpires: the dedup table must not grow without bound; entries
// older than the window are forgotten so a reused id is handled again.
func TestDedupWindowExpires(t *testing.T) {
	d := newDeduper(50 * time.Millisecond)
	defer d.Close()

	if d.Seen("k") {
		t.Fatal("fresh id reported as seen")
	}
	if !d.Seen("k") {
		t.Fatal("second sighting not reported as duplicate")
	}
	time.Sleep(120 * time.Millisecond)
	if d.Seen("k") {
		t.Fatal("id still remembered after the window expired")
	}
}

// TestDeduperIsBounded: a flood of distinct ids must not grow memory forever.
func TestDeduperIsBounded(t *testing.T) {
	d := newDeduper(time.Hour)
	defer d.Close()
	d.MaxEntries = 100
	for i := 0; i < 1000; i++ {
		d.Seen(string(rune('a'+i%26)) + string(rune('0'+i%10)) + string(rune('A'+i%26)))
	}
	d.mu.Lock()
	n := len(d.seen)
	d.mu.Unlock()
	if n > d.MaxEntries {
		t.Fatalf("dedup table grew to %d, limit %d", n, d.MaxEntries)
	}
}

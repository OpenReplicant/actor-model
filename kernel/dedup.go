package kernel

import (
	"encoding/json"
	"sync"
	"time"
)

// JetStream is at-least-once: a consumer that dies between handling a message
// and acking it will see that message again. That is the correct guarantee for
// a durable log, but it means a handler can run twice for one message, and a
// handler that is not idempotent will corrupt state on the second run.
//
// A deduper closes that hole for actors that opt in. It remembers recently seen
// message ids for a bounded window; a repeated id is dropped before reaching
// Handle. This is standard inbox-pattern deduplication, not exactly-once
// delivery — exactly-once across a network is not achievable — but it turns
// "your handler must be idempotent" from a warning into an enforced behaviour
// for the window it covers.
//
// The id comes from the message envelope: a JSON body with an "id" field. A
// message without one cannot be deduplicated and is passed through, since
// dropping unknown messages would be worse than handling a duplicate.

// dedupWindow is how long a message id is remembered by default.
const dedupWindow = 5 * time.Minute

// dedupMaxEntries bounds the table so a flood of distinct ids cannot exhaust
// memory. When exceeded the oldest quarter is evicted.
const dedupMaxEntries = 100_000

// deduper remembers recently seen message ids for a bounded time window.
type deduper struct {
	mu     sync.Mutex
	window time.Duration
	seen   map[string]time.Time
	order  []string // insertion order, for eviction
	closed bool
	stopCh chan struct{}

	// MaxEntries bounds the table size. Zero means the package default.
	MaxEntries int
}

// newDeduper creates a deduper with the given window and starts its janitor.
func newDeduper(window time.Duration) *deduper {
	if window <= 0 {
		window = dedupWindow
	}
	d := &deduper{
		window:     window,
		seen:       make(map[string]time.Time),
		MaxEntries: dedupMaxEntries,
		stopCh:     make(chan struct{}),
	}
	go d.janitor()
	return d
}

// Seen records id and reports whether it had already been seen inside the
// window. A false return means "not a duplicate; deliver it".
func (d *deduper) Seen(id string) bool {
	if id == "" {
		return false // nothing to dedup against
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	now := time.Now()
	if at, ok := d.seen[id]; ok && now.Sub(at) < d.window {
		return true
	}
	d.seen[id] = now
	d.order = append(d.order, id)
	if d.MaxEntries > 0 && len(d.seen) > d.MaxEntries {
		d.evictLocked()
	}
	return false
}

// evictLocked drops the oldest quarter of entries. Callers must hold d.mu.
func (d *deduper) evictLocked() {
	drop := len(d.order) / 4
	if drop == 0 {
		drop = 1
	}
	for i := 0; i < drop && i < len(d.order); i++ {
		delete(d.seen, d.order[i])
	}
	d.order = d.order[drop:]
}

// janitor periodically drops expired ids so an idle deduper does not hold
// memory for messages nobody will resend.
func (d *deduper) janitor() {
	ticker := time.NewTicker(d.window / 4)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case now := <-ticker.C:
			d.mu.Lock()
			kept := d.order[:0]
			for _, id := range d.order {
				if at, ok := d.seen[id]; ok && now.Sub(at) < d.window {
					kept = append(kept, id)
					continue
				}
				delete(d.seen, id)
			}
			d.order = kept
			d.mu.Unlock()
		}
	}
}

// Close stops the janitor. Idempotent.
func (d *deduper) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.mu.Unlock()
	close(d.stopCh)
}

// messageID extracts the envelope id from a JSON message body. It returns ""
// when the body is not JSON or carries no id.
func messageID(data []byte) string {
	if len(data) == 0 || data[0] != '{' {
		return ""
	}
	var env struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return ""
	}
	return env.ID
}

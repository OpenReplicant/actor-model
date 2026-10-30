package kernel

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPerActorMetricsAreIsolated guards against per-actor metrics sharing one
// underlying value. ActorMetrics(id) used to resolve each counter through the
// name-keyed m.cust map, so every actor's "handles" counter pointed at the same
// customMetric: the output looked per-actor (distinct `actor=` labels) while
// every actor reported the same number.
func TestPerActorMetricsAreIsolated(t *testing.T) {
	m := NewMetrics()
	alpha := m.ActorMetrics("alpha")
	beta := m.ActorMetrics("beta")

	for i := 0; i < 3; i++ {
		alpha.Handles.Inc()
	}
	beta.Handles.Inc()

	if got, want := alpha.Handles.value(), 3.0; got != want {
		t.Fatalf("alpha handles = %v, want %v", got, want)
	}
	if got, want := beta.Handles.value(), 1.0; got != want {
		t.Fatalf("beta handles = %v, want %v (metrics must not be shared by name)", got, want)
	}
}

// TestActorMetricsLookupIsIdempotent verifies repeated lookups return the same
// accounting, not a fresh zeroed instance.
func TestActorMetricsLookupIsIdempotent(t *testing.T) {
	m := NewMetrics()
	m.ActorMetrics("same").Handles.Inc()
	if got := m.ActorMetrics("same").Handles.value(); got != 1 {
		t.Fatalf("handles after re-lookup = %v, want 1", got)
	}
}

// TestMetricsTextFormatIsWellFormed checks the exposition output for the two
// defects that made it unparseable and misleading:
//   - a metric name repeated with no label set (the unlabelled per-actor
//     series), which collides with the labelled one
//   - duplicate HELP/TYPE lines for a single metric name
func TestMetricsTextFormatIsWellFormed(t *testing.T) {
	m := NewMetrics()
	m.ActorMetrics("alpha").Handles.Inc()
	out := m.TextFormat()

	helpSeen := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "# HELP ") {
			continue
		}
		name := strings.Fields(line)[2]
		if helpSeen[name] {
			t.Fatalf("duplicate HELP for %q (Prometheus rejects a second HELP/TYPE for one name):\n%s", name, out)
		}
		helpSeen[name] = true
	}

	// Every per-actor series must carry the actor label; there must be no bare
	// series for a per-actor metric name.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		name := line[:strings.IndexAny(line, " {")]
		if strings.HasPrefix(name, "actor_kernel_actor_") && !strings.Contains(line, `{actor="`) {
			t.Fatalf("per-actor metric %q emitted without an actor label: %q", name, line)
		}
	}
}

// TestMetricsConcurrentScrapeAndRegistration reproduces the lock-order
// inversion between TextFormat (m.mu -> actorMu) and ActorMetrics
// (actorMu -> m.mu). Before the fix this deadlocked permanently, taking the
// whole kernel with it because every Inc() blocks on the same m.mu.
func TestMetricsConcurrentScrapeAndRegistration(t *testing.T) {
	m := NewMetrics()
	m.ActorMetrics("warmup") // ensure the path is exercised, not just map growth

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			m.TextFormat()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			m.ActorMetrics("actor-" + string(rune('a'+i%26))).Handles.Inc()
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: concurrent scrape and actor registration blocked each other")
	}
	wg.Wait()
}

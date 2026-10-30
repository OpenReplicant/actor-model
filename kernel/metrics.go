// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"sort"
	"strconv"
	"sync"
)

// Metrics holds process-level and per-actor counters/gauges exposed in
// Prometheus text exposition format. All methods are safe for concurrent use.
//
// These metrics are intentionally minimal and dependency-free: they are plain
// counters/gauges/histograms formatted to the Prometheus text exposition format
// and can be scraped by any Prometheus-compatible scraper. Replace with the
// official Prometheus client library when network access allows
// `go get github.com/prometheus/client_golang`.
//
// Per-actor metrics are created on demand via ActorMetrics(id) and are keyed
// by actor id. They are rendered with an `actor="..."` label so each actor's
// values can be scraped independently.
type Metrics struct {
	mu sync.Mutex

	// Pre-registered kernel metrics used by the registry delivery path.
	MessagesPublished  *CounterMetric
	MessagesReceived   *CounterMetric
	ActorsRunning      *GaugeMetric
	HandleDuration     *GaugeMetric     // last handle duration in seconds (approximate)
	HandleDurationHist *HistogramMetric // distribution of handle durations

	cust map[string]*customMetric

	actorMu      sync.Mutex
	actorMetrics map[string]*ActorMetrics
}

// ActorMetrics holds the per-actor observation metrics. One is created per
// actor id the first time ActorMetrics(id) is called.
//
// These are deliberately *not* resolved through the name-keyed m.cust map.
// Doing so made every actor share one underlying value: each actor got its own
// {actor="..."} label over the same number, so the exposition looked
// per-actor while every actor reported identical figures.
type ActorMetrics struct {
	id string

	MessagesReceived *CounterMetric // messages delivered to this actor's mailbox
	Handles          *CounterMetric // Handle calls dispatched for this actor
	HandleDuration   *GaugeMetric   // last handle duration in seconds (approximate)
	Dropped          *CounterMetric // messages dropped because the actor's mailbox was full
	MailboxDepth     *GaugeMetric   // current depth of this actor's inbox (messages)

	// Panics counts handler panics recovered by the delivery goroutine.
	Panics *CounterMetric
}

// value returns the counter's current value. Each metric has its own mutex, so
// reading takes that mutex rather than relying on the caller to hold a coarser
// one.
func (cm *CounterMetric) value() float64 {
	mu := cm.metricMu()
	mu.Lock()
	defer mu.Unlock()
	return cm.c.val
}

// value returns the gauge's current value (see CounterMetric.value).
func (gm *GaugeMetric) value() float64 {
	mu := gm.metricMu()
	mu.Lock()
	defer mu.Unlock()
	return gm.c.val
}

// NewMetrics creates a Metrics registry with the standard kernel metrics
// pre-registered.
func NewMetrics() *Metrics {
	m := &Metrics{
		cust:         make(map[string]*customMetric),
		actorMetrics: make(map[string]*ActorMetrics),
	}
	m.MessagesPublished = m.Counter("actor_kernel_messages_published_total", "Total number of messages published by the registry")
	m.MessagesReceived = m.Counter("actor_kernel_messages_received_total", "Total number of messages received by actors")
	m.ActorsRunning = m.Gauge("actor_kernel_actors_running", "Number of actors currently running (delivery goroutines active)")
	// Sparkline of the most recent handle duration. It must not share a name
	// with the histogram below: Prometheus rejects a metric name exported with
	// two different types, and that collision made the whole scrape invalid.
	m.HandleDuration = m.Gauge("actor_kernel_last_handle_duration_seconds", "Duration of the most recent Handle call in seconds")
	m.HandleDurationHist = NewHistogram("actor_kernel_handle_duration_seconds", "Histogram of actor handle durations",
		[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5})
	return m
}

// ActorMetrics returns (creating if needed) the per-actor metrics for the
// given actor id. The same instance is returned for repeated calls with the
// same id.
// Lock order: ActorMetrics holds only actorMu and allocates plain values, so it
// never reaches for m.mu. TextFormat takes m.mu and then actorMu. Keeping the
// two dedicated sets of mutexes strictly ordered (m.mu -> actorMu, never the
// reverse) is what prevents a scrape racing an actor start from deadlocking the
// whole kernel.
func (m *Metrics) ActorMetrics(id string) *ActorMetrics {
	m.actorMu.Lock()
	defer m.actorMu.Unlock()
	am, ok := m.actorMetrics[id]
	if !ok {
		am = &ActorMetrics{
			id:               id,
			MessagesReceived: newCounter("actor_kernel_actor_messages_received_total", "Total number of messages received by actor"),
			Handles:          newCounter("actor_kernel_actor_handles_total", "Total number of Handle calls dispatched for actor"),
			HandleDuration:   newGauge("actor_kernel_actor_handle_duration_seconds", "Last handle duration in seconds for actor"),
			Dropped:          newCounter("actor_kernel_actor_messages_dropped_total", "Total number of messages dropped for actor (mailbox full)"),
			MailboxDepth:     newGauge("actor_kernel_actor_mailbox_depth", "Current depth of actor's inbox (messages)"),
			Panics:           newCounter("actor_kernel_actor_panics_total", "Total number of handler panics recovered for actor"),
		}
		m.actorMetrics[id] = am
	}
	return am
}

// Counter returns (creating if needed) a counter metric by name.
func (m *Metrics) Counter(name, help string) *CounterMetric {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cust[name]
	if !ok {
		c = &customMetric{mu: &m.mu, name: name, typ: "counter", help: help}
		m.cust[name] = c
	}
	return &CounterMetric{m: m, c: c}
}

// Gauge returns (creating if needed) a gauge metric by name.
func (m *Metrics) Gauge(name, help string) *GaugeMetric {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cust[name]
	if !ok {
		c = &customMetric{mu: &m.mu, name: name, typ: "gauge", help: help}
		m.cust[name] = c
	}
	return &GaugeMetric{m: m, c: c}
}

// newCounter allocates a counter that is not registered in the name-keyed
// m.cust map. Used for per-actor series, which are rendered separately with an
// actor label and must keep their own storage.
func newCounter(name, help string) *CounterMetric {
	return &CounterMetric{c: &customMetric{mu: &sync.Mutex{}, name: name, typ: "counter", help: help}}
}

// newGauge allocates a gauge that is not registered in m.cust, for the same
// reason as newCounter.
func newGauge(name, help string) *GaugeMetric {
	return &GaugeMetric{c: &customMetric{mu: &sync.Mutex{}, name: name, typ: "gauge", help: help}}
}

// CounterMetric is an increment-only counter.
type CounterMetric struct {
	m *Metrics
	c *customMetric
}

// metricMu returns the mutex guarding this metric's value. Registered metrics
// (m.cust) are guarded by the shared Metrics mutex; per-actor series allocate
// their own, so that a hot per-actor increment never serialises against a
// scrape or against every other actor on the bus.
func (cm *CounterMetric) metricMu() *sync.Mutex { return cm.c.mu }

// Inc increments the counter by 1.
func (cm *CounterMetric) Inc() {
	mu := cm.metricMu()
	mu.Lock()
	cm.c.val++
	mu.Unlock()
}

// Add adds v to the counter.
func (cm *CounterMetric) Add(v float64) {
	mu := cm.metricMu()
	mu.Lock()
	cm.c.val += v
	mu.Unlock()
}

// GaugeMetric is a gauge that can go up and down.
type GaugeMetric struct {
	m *Metrics
	c *customMetric
}

// metricMu returns the mutex guarding this gauge's value (see
// CounterMetric.metricMu).
func (gm *GaugeMetric) metricMu() *sync.Mutex { return gm.c.mu }

// Inc increments the gauge by 1.
func (gm *GaugeMetric) Inc() {
	mu := gm.metricMu()
	mu.Lock()
	gm.c.val++
	mu.Unlock()
}

// Dec decrements the gauge by 1.
func (gm *GaugeMetric) Dec() {
	mu := gm.metricMu()
	mu.Lock()
	gm.c.val--
	mu.Unlock()
}

// Set sets the gauge to v.
func (gm *GaugeMetric) Set(v float64) {
	mu := gm.metricMu()
	mu.Lock()
	gm.c.val = v
	mu.Unlock()
}

// HistogramMetric is a simple dependency-free histogram. Observe records a
// duration; TextFormat renders it as Prometheus histogram buckets.
type HistogramMetric struct {
	mu      sync.Mutex
	name    string
	help    string
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
}

// NewHistogram creates a HistogramMetric with the given buckets (in seconds).
func NewHistogram(name, help string, buckets []float64) *HistogramMetric {
	b := make([]float64, len(buckets))
	copy(b, buckets)
	sort.Float64s(b)
	return &HistogramMetric{
		name:    name,
		help:    help,
		buckets: b,
		counts:  make([]int64, len(b)+1), // +1 for +Inf
	}
}

// Observe records a duration in seconds.
func (h *HistogramMetric) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.count++
	// find the first bucket where v <= le
	le := len(h.buckets)
	for i, b := range h.buckets {
		if v <= b {
			le = i
			break
		}
	}
	h.counts[le]++
}

// customMetric holds a single named metric with a type, help text, and value.
//
// A customMetric is either registered in Metrics.cust (guarded by
// Metrics.mu, which mu points at) or owned by one per-actor series (guarded by
// its own mu). See newCounter/newGauge.
type customMetric struct {
	mu   *sync.Mutex
	name string
	typ  string // "counter" or "gauge"
	help string
	val  float64
}

// TextFormat returns the metrics in Prometheus text exposition format.
//
// Lock order is m.mu then m.actorMu. ActorMetrics only ever takes actorMu and
// allocates its own values, so the reverse order never happens.
func (m *Metrics) TextFormat() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b []byte
	// Sort names so output is deterministic (each name appears exactly once).
	names := make([]string, 0, len(m.cust))
	for name := range m.cust {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		c := m.cust[name]
		b = append(b, "# HELP "+name+" "+c.help+"\n"...)
		b = append(b, "# TYPE "+name+" "+c.typ+"\n"...)
		b = append(b, name+" "+formatFloat(c.val)+"\n"...)
	}

	m.actorMu.Lock()
	b = appendActorMetrics(m.actorMetrics, b)
	m.actorMu.Unlock()

	// Handle duration histogram. Its own mutex is acquired after both of the
	// above are released, so it can never participate in a cycle.
	b = appendHistogram(m.HandleDurationHist, b)
	return string(b)
}

// appendActorMetrics renders the per-actor series. Each metric name gets its
// HELP/TYPE header once, followed by one labelled sample per actor — the
// exposition format forbids a second HELP/TYPE for the same name, which the
// previous per-actor-header-per-actor rendering produced.
//
// Values are read without their individual mutexes because rendering holds
// m.actorMu, which guards every per-actor series.
func appendActorMetrics(byActor map[string]*ActorMetrics, b []byte) []byte {
	if len(byActor) == 0 {
		return b
	}
	ids := make([]string, 0, len(byActor))
	for id := range byActor {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	type series struct {
		name string
		help string
		typ  string
		get  func(*ActorMetrics) float64
	}
	seriesList := []series{
		{"actor_kernel_actor_messages_received_total", "Total messages received by actor", "counter",
			func(am *ActorMetrics) float64 { return am.MessagesReceived.value() }},
		{"actor_kernel_actor_handles_total", "Total Handle calls dispatched for actor", "counter",
			func(am *ActorMetrics) float64 { return am.Handles.value() }},
		{"actor_kernel_actor_handle_duration_seconds", "Last handle duration in seconds for actor", "gauge",
			func(am *ActorMetrics) float64 { return am.HandleDuration.value() }},
		{"actor_kernel_actor_messages_dropped_total", "Total messages dropped for actor (mailbox full)", "counter",
			func(am *ActorMetrics) float64 { return am.Dropped.value() }},
		{"actor_kernel_actor_mailbox_depth", "Current depth of actor's inbox (messages)", "gauge",
			func(am *ActorMetrics) float64 { return am.MailboxDepth.value() }},
		{"actor_kernel_actor_panics_total", "Total handler panics recovered for actor", "counter",
			func(am *ActorMetrics) float64 { return am.Panics.value() }},
	}

	for _, s := range seriesList {
		b = append(b, "# HELP "+s.name+" "+s.help+"\n"...)
		b = append(b, "# TYPE "+s.name+" "+s.typ+"\n"...)
		for _, id := range ids {
			b = append(b, s.name+`{actor="`+escapeLabel(id)+`"} `+formatFloat(s.get(byActor[id]))+"\n"...)
		}
	}
	return b
}

// appendHistogram renders a HistogramMetric in Prometheus exposition format.
func appendHistogram(h *HistogramMetric, b []byte) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	b = append(b, "# HELP "+h.name+" "+h.help+"\n"...)
	b = append(b, "# TYPE "+h.name+" histogram\n"...)
	cumulative := int64(0)
	for i, le := range h.buckets {
		cumulative += h.counts[i]
		b = append(b, h.name+"_bucket{le=\""+strconv.FormatFloat(le, 'f', -1, 64)+"\"} "+strconv.FormatInt(cumulative, 10)+"\n"...)
	}
	cumulative += h.counts[len(h.buckets)] // +Inf bucket
	b = append(b, h.name+"_bucket{le=\"+Inf\"} "+strconv.FormatInt(cumulative, 10)+"\n"...)
	b = append(b, h.name+"_sum "+strconv.FormatFloat(h.sum, 'f', -1, 64)+"\n"...)
	b = append(b, h.name+"_count "+strconv.FormatInt(h.count, 10)+"\n"...)
	return b
}

// escapeLabel escapes backslashes, double quotes, and newlines for a Prometheus label value.
func escapeLabel(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			out = append(out, '\\', '\\')
		case '"':
			out = append(out, '\\', '"')
		case '\n':
			out = append(out, '\\', 'n')
		default:
			out = append(out, s[i])
		}
	}
	return string(out)
}

// formatFloat formats v without unnecessary trailing zeros.
func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// ensure Metrics is usable at compile time.
var _ interface{ TextFormat() string } = (*Metrics)(nil)

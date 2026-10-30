// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
)

//go:embed index.html
var webuiHTML string

// webUIRegistry is the subset of Registry the WebUIActor needs: publish,
// list actors, and read metrics. *Registry satisfies it.
type webUIRegistry interface {
	Publish(subject string, data []byte) error
	ListActors() []ActorInfo
	Metrics() *Metrics
}

// WebUIActor serves an admin dashboard over HTTP at its configured address and
// handles NATS-based UI requests. NATS itself is the bus — the browser connects
// over the embedded server's WebSocket endpoint and publishes directly. The
// WebUIActor only mediates reads of actor state the browser cannot get from
// NATS directly:
//   - webui.list.actors  → responds with JSON actor list on msg.Reply
//   - webui.metrics      → responds with Prometheus metrics text on msg.Reply
//
// Writes (publishes) the browser does directly via nc.publish(subject, data),
// making the dashboard a first-class actor-network participant.
type WebUIActor struct {
	id     string
	topics []string
	reg    webUIRegistry
	addr   string
	wsURL  string

	mu   sync.Mutex
	once sync.Once
}

// NewWebUIActor creates a web UI actor served at addr, with wsURL injected
// into the dashboard so the browser can connect to the NATS WebSocket endpoint.
func NewWebUIActor(id string, topics []string, addr string, wsURL string, reg webUIRegistry) *WebUIActor {
	return &WebUIActor{id: id, topics: topics, addr: addr, wsURL: wsURL, reg: reg}
}

func (a *WebUIActor) ID() string       { return a.id }
func (a *WebUIActor) Topics() []string { return a.topics }

func (a *WebUIActor) Handle(ctx context.Context, msg Msg) {
	// Start the HTTP dashboard server on the first message.
	a.once.Do(func() {
		if a.addr != "" {
			go a.serveHTTP()
		}
	})

	switch msg.Subject {
	case "webui.list.actors":
		actors := a.reg.ListActors()
		b, _ := json.Marshal(actors)
		if msg.Reply != "" {
			_ = a.reg.Publish(msg.Reply, b)
		}
	case "webui.metrics":
		text := a.reg.Metrics().TextFormat()
		if msg.Reply != "" {
			_ = a.reg.Publish(msg.Reply, []byte(text))
		}
	}
}

func (a *WebUIActor) serveHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleDashboard)
	mux.HandleFunc("/api/actors", a.handleActors)
	mux.HandleFunc("/api/metrics", a.handleMetrics)

	if err := http.ListenAndServe(a.addr, mux); err != nil {
		log.Printf("webui %s: http server: %v", a.id, err)
	}
}

func (a *WebUIActor) handleDashboard(w http.ResponseWriter, r *http.Request) {
	html := strings.Replace(webuiHTML, "{{WS_URL}}", a.wsURL, 1)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, html)
}

func (a *WebUIActor) handleActors(w http.ResponseWriter, r *http.Request) {
	actors := a.reg.ListActors()
	b, _ := json.Marshal(actors)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, string(b))
}

func (a *WebUIActor) handleMetrics(w http.ResponseWriter, r *http.Request) {
	text := a.reg.Metrics().TextFormat()
	w.Header().Set("Content-Type", `text/plain; version=0.0.4; charset=utf-8`)
	io.WriteString(w, text)
}

// ensure WebUIActor implements Actor at compile time.
var _ Actor = (*WebUIActor)(nil)

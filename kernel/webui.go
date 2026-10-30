// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var webuiHTML string

// webUIRegistry is the subset of Registry the WebUIActor needs: publish,
// list actors, read metrics, and — when ownership is on — report leases so the
// dashboard can show who owns what. *Registry satisfies it.
type webUIRegistry interface {
	Publish(subject string, data []byte) error
	ListActors() []ActorInfo
	Metrics() *Metrics
	Ownership() (*LeaseStore, string, bool)
}

// SchedulerReporter is implemented by the scheduler actor. The dashboard asks
// the registry for it so scheduler state is a read, not a race with the
// scheduler's own actor.
type SchedulerReporter interface {
	Plans() []SchedulerStatus
}

// schedulerHolder is implemented by registries that can point at the scheduler
// running on this node.
type schedulerHolder interface {
	Scheduler() SchedulerReporter
}

// WebUIActor serves an admin dashboard over HTTP at its configured address and
// handles NATS-based UI requests. NATS itself is the bus — the browser connects
// over the embedded server's WebSocket endpoint and publishes directly. The
// WebUIActor only mediates reads of actor state the browser cannot derive from
// NATS on its own:
//   - webui.list.actors  → JSON actor list
//   - webui.metrics      → Prometheus text
//   - webui.owners       → ownership leases (who owns which actor id)
//   - webui.scheduler    → scheduler decisions on this node
//
// Writes (publishes) the browser does directly via nc.publish(subject, data),
// making the dashboard a first-class actor-network participant. Lifecycle
// control is exactly that: supervision.start.<id> / supervision.stop.<id> are
// consumed by the scheduler, not by this actor.
type WebUIActor struct {
	id     string
	topics []string
	reg    webUIRegistry
	addr   string
	wsURL  string
	nodeID string
	// auth is rendered into the page so the browser can authenticate to the
	// bus. Only safe over a trusted network: anything the page can read, the
	// viewer can read.
	auth AuthConfig

	mu   sync.Mutex
	once sync.Once
	srv  *http.Server
	done chan struct{}
}

// NewWebUIActor creates a web UI actor served at addr, with wsURL injected into
// the dashboard so the browser can connect to the NATS WebSocket endpoint.
func NewWebUIActor(id string, topics []string, addr string, wsURL string, reg webUIRegistry) *WebUIActor {
	return &WebUIActor{id: id, topics: topics, addr: addr, wsURL: wsURL, reg: reg}
}

// WithNodeID sets the node identity shown in the dashboard header.
func (a *WebUIActor) WithNodeID(node string) *WebUIActor {
	a.nodeID = node
	return a
}

// WithBrowserAuth supplies the credentials the dashboard page uses to connect.
// The browser cannot read env vars or flags, so the only options are to inject
// them into the page or to have the operator paste them in. Injecting trusts
// whoever can load the page; that is acceptable for an admin dashboard on a
// private network and not acceptable on the open internet — put it behind a
// reverse proxy that authenticates then.
func (a *WebUIActor) WithBrowserAuth(auth AuthConfig) *WebUIActor {
	a.auth = auth
	return a
}

func (a *WebUIActor) ID() string       { return a.id }
func (a *WebUIActor) Topics() []string { return a.topics }

func (a *WebUIActor) Handle(ctx context.Context, msg Msg) {
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
	case "webui.owners":
		b, _ := json.Marshal(a.ownersPayload())
		if msg.Reply != "" {
			_ = a.reg.Publish(msg.Reply, b)
		}
	case "webui.scheduler":
		b, _ := json.Marshal(a.schedulerPayload())
		if msg.Reply != "" {
			_ = a.reg.Publish(msg.Reply, b)
		}
	}
}

// ownersPayload reports who owns which actor id, or that ownership is off.
func (a *WebUIActor) ownersPayload() map[string]interface{} {
	leases, node, enabled := a.reg.Ownership()
	if !enabled {
		return map[string]interface{}{"enabled": false, "node": "", "leases": []Lease{}}
	}
	list, err := leases.List(context.Background())
	if err != nil {
		return map[string]interface{}{"enabled": true, "node": node, "leases": []Lease{}, "error": err.Error()}
	}
	return map[string]interface{}{"enabled": true, "node": node, "leases": list}
}

// schedulerPayload returns the scheduler's current view, or an empty list when
// this node runs no scheduler.
func (a *WebUIActor) schedulerPayload() []SchedulerStatus {
	h, ok := a.reg.(schedulerHolder)
	if !ok || h.Scheduler() == nil {
		return []SchedulerStatus{}
	}
	return h.Scheduler().Plans()
}

// Start opens the dashboard listener. Called by Registry.Start rather than
// lazily on first message, because a dashboard nobody publishes to would
// otherwise never come up at all.
func (a *WebUIActor) Start() error {
	if a.addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleDashboard)
	mux.HandleFunc("/api/actors", a.handleActors)
	mux.HandleFunc("/api/metrics", a.handleMetrics)
	mux.HandleFunc("/api/owners", a.handleJSON(func() interface{} { return a.ownersPayload() }))
	mux.HandleFunc("/api/scheduler", a.handleJSON(func() interface{} { return a.schedulerPayload() }))
	mux.HandleFunc("/api/publish", a.handlePublish)

	srv := &http.Server{Addr: a.addr, Handler: mux}
	a.mu.Lock()
	a.srv = srv
	a.done = make(chan struct{})
	a.mu.Unlock()

	go func() {
		defer close(a.done)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("webui %s: http server: %v", a.id, err)
		}
	}()
	return nil
}

// Stop shuts the dashboard listener down. Safe to call more than once.
func (a *WebUIActor) Stop() error {
	a.mu.Lock()
	srv, done := a.srv, a.done
	a.srv = nil
	a.mu.Unlock()
	if srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := srv.Shutdown(ctx)
	if done != nil {
		<-done
	}
	return err
}

// handlePublish lets the dashboard publish without a NATS connection, which is
// useful when the browser cannot reach the WebSocket endpoint (TLS, proxy, or
// a locked-down network). The message goes onto the same bus with the same
// credentials the kernel holds, so it is not a privilege escalation — it is a
// second transport to the same rights.
func (a *WebUIActor) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Subject string `json:"subject"`
		Data    string `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if body.Subject == "" {
		http.Error(w, "subject required", http.StatusBadRequest)
		return
	}
	if err := a.reg.Publish(body.Subject, []byte(body.Data)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *WebUIActor) handleDashboard(w http.ResponseWriter, r *http.Request) {
	html := strings.Replace(webuiHTML, "{{WS_URL}}", a.wsURL, 1)
	html = strings.Replace(html, "{{NODE_ID}}", a.nodeID, 1)
	html = strings.Replace(html, "{{AUTH}}", a.browserAuthJSON(), 1)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, html)
}

// browserAuthJSON renders the NATS client options the page should pass to
// new NATS(...). Secrets are JSON-encoded so quoting cannot break the page.
func (a *WebUIActor) browserAuthJSON() string {
	opts := map[string]string{}
	switch a.auth.Method {
	case AuthUserPass:
		opts["user"], opts["pass"] = a.auth.Username, a.auth.Password
	case AuthToken:
		opts["token"] = a.auth.Token
	case AuthUsers:
		// The dashboard should not use an admin credential, but if it must, use
		// the one configured for the kernel's own connection.
		if u, err := a.auth.connectUser(); err == nil {
			opts["user"], opts["pass"] = u.Username, u.Password
		}
	}
	b, _ := json.Marshal(opts)
	return string(b)
}

func (a *WebUIActor) handleActors(w http.ResponseWriter, r *http.Request) {
	actors := a.reg.ListActors()
	b, _ := json.Marshal(actors)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, string(b))
}

// handleJSON serves any payload builder as JSON.
func (a *WebUIActor) handleJSON(build func() interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := json.Marshal(build())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}
}

func (a *WebUIActor) handleMetrics(w http.ResponseWriter, r *http.Request) {
	text := a.reg.Metrics().TextFormat()
	w.Header().Set("Content-Type", `text/plain; version=0.0.4; charset=utf-8`)
	io.WriteString(w, text)
}

// ensure WebUIActor implements Actor and Runnable at compile time.
var _ Actor = (*WebUIActor)(nil)
var _ Runnable = (*WebUIActor)(nil)

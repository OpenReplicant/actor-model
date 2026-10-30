package actors

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	kernel "actor-kernel/kernel"
)

// ClientCredentials identifies a browser client at registration time.
//
// Note that this is *not* the authentication boundary. A browser must already
// hold valid NATS credentials (whatever method the bus is configured with — see
// NATS auth in natsauth.go) to publish on a webclient subject at all. This
// struct carries the client's declared identity so the actor can associate a
// session with a name; authorization is enforced by NATS subject permissions.
//
// An earlier version re-validated a password here and exposed an Authorize
// method that was never called. That implied protection the system did not
// have. Identity here, authorization in NATS.
type ClientCredentials struct {
	// Username is the client's declared identity, for session association.
	Username string `json:"username,omitempty"`
	// Token carries an OIDC/JWT subject when the bus authenticates by token or
	// creds; the actor treats it as an opaque identifier.
	Token string `json:"token,omitempty"`
}

// WebClientActor represents a browser-based worker node on the NATS bus.
// The browser connects to NATS over WebSocket and acts as a first-class
// actor: it subscribes to topics, handles messages, and publishes results.
// The WebClientActor manages the browser's registration, lifecycle, and
// topic routing on the kernel side.
//
// Subscribes to webclient.<id>.>:
//   - webclient.<id>.register  → browser registers (body: ClientCredentials JSON)
//   - webclient.<id>.heartbeat → browser liveness check (empty body)
//   - webclient.<id>.disconnect → browser signals it is going away
//   - webclient.<id>.task      → task for the browser (forwarded to webclient.<id>.incoming)
//   - webclient.<id>.result    → result published by the browser
//
// The actor publishes webclient.<id>.status (online/offline/error) and
// webclient.<id>.incoming (tasks forwarded to the browser).
//
// Authentication happens at the bus, not here: a browser needs working NATS
// credentials to reach these subjects at all, and NATS subject permissions are
// what restrict which subjects a given client may use. The actor records the
// declared identity from the register message and tracks session liveness.
type WebClientActor struct {
	id     string
	topics []string
	reg    kernel.Publisher
	creds  ClientCredentials

	mu           sync.Mutex
	connected    bool
	registeredAt time.Time
	lastSeen     time.Time
}

// NewWebClientActor creates a web client actor for the given browser id.
func NewWebClientActor(id string, topics []string, reg kernel.Publisher) *WebClientActor {
	return &WebClientActor{
		id:     id,
		topics: topics,
		reg:    reg,
	}
}

func (a *WebClientActor) ID() string       { return a.id }
func (a *WebClientActor) Topics() []string { return a.topics }

func (a *WebClientActor) Handle(ctx context.Context, msg kernel.Msg) {
	switch msg.Subject {
	case "webclient." + a.id + ".register":
		a.handleRegister(msg)
	case "webclient." + a.id + ".heartbeat":
		a.handleHeartbeat()
	case "webclient." + a.id + ".disconnect":
		a.handleDisconnect()
	case "webclient." + a.id + ".task":
		a.handleTask(msg)
	case "webclient." + a.id + ".result":
		a.handleResult(msg)
	}
}

func (a *WebClientActor) handleRegister(msg kernel.Msg) {
	var creds ClientCredentials
	if err := json.Unmarshal(msg.Data, &creds); err != nil {
		a.publishStatus("error", "invalid registration payload")
		return
	}
	a.mu.Lock()
	a.connected = true
	a.registeredAt = time.Now()
	a.lastSeen = time.Now()
	a.creds = creds
	a.mu.Unlock()
	a.publishStatus("online", "")
}

func (a *WebClientActor) handleHeartbeat() {
	a.mu.Lock()
	a.lastSeen = time.Now()
	a.connected = true
	a.mu.Unlock()
}

func (a *WebClientActor) handleDisconnect() {
	a.mu.Lock()
	a.connected = false
	a.mu.Unlock()
	a.publishStatus("offline", "")
}

func (a *WebClientActor) handleTask(msg kernel.Msg) {
	a.mu.Lock()
	if !a.connected {
		a.mu.Unlock()
		return // drop task if browser not connected
	}
	a.mu.Unlock()
	// Forward task to the browser's inbound topic.
	_ = a.reg.Publish("webclient."+a.id+".incoming", msg.Data)
}

func (a *WebClientActor) handleResult(msg kernel.Msg) {
	// Browser published a result; other actors can subscribe to this topic.
	a.mu.Lock()
	a.lastSeen = time.Now()
	a.mu.Unlock()
}

func (a *WebClientActor) publishStatus(status, reason string) {
	payload, _ := json.Marshal(map[string]string{
		"id":     a.id,
		"status": status,
		"reason": reason,
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
	_ = a.reg.Publish("webclient."+a.id+".status", payload)
}

// Connected returns whether the browser client is currently registered.
func (a *WebClientActor) Connected() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.connected
}

// LastSeen returns the last time the browser client sent a heartbeat or result.
func (a *WebClientActor) LastSeen() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastSeen
}

// ensure WebClientActor implements Actor at compile time.
var _ kernel.Actor = (*WebClientActor)(nil)

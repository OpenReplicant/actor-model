// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// ClientCredentials holds authentication information for a web client.
// Token is reserved for a future OIDC swap; username/password is basic NATS auth.
type ClientCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Token    string `json:"token"` // reserved for OIDC
}

// Authenticator verifies a web client's identity and topic authorization.
// Basic NATS password auth is the default; an OIDC implementation can be
// swapped in later without changing the WebClientActor.
type Authenticator interface {
	// Authenticate reports whether the client's credentials are valid.
	Authenticate(id string, creds ClientCredentials) bool
	// Authorize reports whether the client may access the given subject.
	Authorize(id string, subject string) bool
}

// BasicAuthenticator authenticates against a single static username/password.
type BasicAuthenticator struct {
	username string
	password string
}

// NewBasicAuthenticator creates a BasicAuthenticator with the given credentials.
func NewBasicAuthenticator(username, password string) *BasicAuthenticator {
	return &BasicAuthenticator{username: username, password: password}
}

func (a *BasicAuthenticator) Authenticate(_ string, creds ClientCredentials) bool {
	return creds.Username == a.username && creds.Password == a.password
}

func (a *BasicAuthenticator) Authorize(_ string, _ string) bool {
	return true // basic auth: any authenticated client may access any subject
}

// NoopAuthenticator allows all clients without authentication.
type NoopAuthenticator struct{}

func (NoopAuthenticator) Authenticate(_ string, _ ClientCredentials) bool { return true }
func (NoopAuthenticator) Authorize(_ string, _ string) bool               { return true }

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
// Auth: basic NATS username/password via BasicAuthenticator for now; designed
// to swap for OIDC token validation later by providing a different
// Authenticator implementation.
type WebClientActor struct {
	id            string
	topics        []string
	reg           Publisher
	authenticator Authenticator
	creds         ClientCredentials

	mu           sync.Mutex
	connected    bool
	registeredAt time.Time
	lastSeen     time.Time
}

// NewWebClientActor creates a web client actor for the given browser id.
// If authenticator is nil, NoopAuthenticator is used.
func NewWebClientActor(id string, topics []string, reg Publisher, authenticator Authenticator) *WebClientActor {
	if authenticator == nil {
		authenticator = NoopAuthenticator{}
	}
	return &WebClientActor{
		id:            id,
		topics:        topics,
		reg:           reg,
		authenticator: authenticator,
	}
}

func (a *WebClientActor) ID() string       { return a.id }
func (a *WebClientActor) Topics() []string { return a.topics }

func (a *WebClientActor) Handle(ctx context.Context, msg Msg) {
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

func (a *WebClientActor) handleRegister(msg Msg) {
	var creds ClientCredentials
	if err := json.Unmarshal(msg.Data, &creds); err != nil {
		a.publishStatus("error", "invalid credentials")
		return
	}
	if !a.authenticator.Authenticate(a.id, creds) {
		a.publishStatus("error", "authentication failed")
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

func (a *WebClientActor) handleTask(msg Msg) {
	a.mu.Lock()
	if !a.connected {
		a.mu.Unlock()
		return // drop task if browser not connected
	}
	a.mu.Unlock()
	// Forward task to the browser's inbound topic.
	_ = a.reg.Publish("webclient."+a.id+".incoming", msg.Data)
}

func (a *WebClientActor) handleResult(msg Msg) {
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
var _ Actor = (*WebClientActor)(nil)

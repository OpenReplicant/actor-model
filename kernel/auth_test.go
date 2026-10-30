package kernel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// authActor is a simple recording actor used to verify in-process actors keep
// working when network auth is enabled.
type authActor struct {
	id     string
	topics []string
	mu     sync.Mutex
	msgs   []Msg
}

func (a *authActor) ID() string       { return a.id }
func (a *authActor) Topics() []string { return a.topics }
func (a *authActor) Handle(_ context.Context, msg Msg) {
	a.mu.Lock()
	a.msgs = append(a.msgs, msg)
	a.mu.Unlock()
}

func (a *authActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.msgs)
}

func TestAuthInProcessActorsBypassPasswordAuth(t *testing.T) {
	// Embedded server listening on a real port with password auth for network
	// clients. In-process actors connect via InProcessServer and bypass auth.
	opts := &server.Options{
		Host:     "127.0.0.1",
		Port:     -1,
		NoLog:    true,
		NoSigs:   true,
		Username: "admin",
		Password: "secret",
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	defer srv.Shutdown()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}

	// In-process connection (kernel actor path) must still work, using the same
	// credentials that network clients use (auth applies to all connections;
	// in-process is not exempt when username/password is configured).
	nc, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(srv), nats.UserInfo("admin", "secret"))
	if err != nil {
		t.Fatalf("in-process connect (should bypass auth): %v", err)
	}
	defer nc.Close()

	reg := NewRegistry(nc)
	a := &authActor{id: "auth-actor", topics: []string{"auth.test.>"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start actor: %v", err)
	}
	if err := reg.Publish("auth.test.hello", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return a.count() == 1 }) {
		t.Fatalf("expected actor to handle message via in-process bus with auth enabled")
	}

	// A network client without credentials must be rejected.
	_, err = nats.Connect(srv.ClientURL())
	if err == nil {
		t.Fatal("expected network connect without credentials to be rejected")
	}

	// A network client with correct credentials must connect.
	nc2, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "secret"))
	if err != nil {
		t.Fatalf("network connect with credentials: %v", err)
	}
	defer nc2.Close()

	// A network client with wrong credentials must be rejected.
	_, err = nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "wrong"))
	if err == nil {
		t.Fatal("expected network connect with wrong password to be rejected")
	}

	// Verify end-to-end: network client publishes, in-process actor receives.
	if err := nc2.Publish("auth.test.from-network", []byte("y")); err != nil {
		t.Fatalf("network publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return a.count() == 2 }) {
		t.Fatalf("expected actor to handle network-published message, got %d", a.count())
	}
}

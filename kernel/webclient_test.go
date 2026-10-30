package kernel

import (
	"encoding/json"
	"testing"
	"time"
)

// basicAuthenticatorTest is a simple authenticator for tests.
type testAuthenticator struct {
	users map[string]string
}

func newTestAuthenticator(users map[string]string) *testAuthenticator {
	return &testAuthenticator{users: users}
}

func (t *testAuthenticator) Authenticate(_ string, creds ClientCredentials) bool {
	pw, ok := t.users[creds.Username]
	return ok && pw == creds.Password
}

func (t *testAuthenticator) Authorize(_ string, _ string) bool { return true }

func TestWebClientRegisterAndHeartbeat(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := NewWebClientActor("browser-1", []string{"webclient.browser-1.>"}, reg, newTestAuthenticator(map[string]string{"admin": "secret"}))
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	// register with correct creds
	body, _ := json.Marshal(ClientCredentials{Username: "admin", Password: "secret"})
	if err := reg.Publish("webclient.browser-1.register", body); err != nil {
		t.Fatalf("publish register: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if !a.Connected() {
		t.Fatalf("expected client to be connected after valid register")
	}

	// wrong creds fail (does not disconnect an already-connected client,
	// just publishes an error status)
	body, _ = json.Marshal(ClientCredentials{Username: "admin", Password: "wrong"})
	if err := reg.Publish("webclient.browser-1.register", body); err != nil {
		t.Fatalf("publish register: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// heartbeat keeps it online
	if err := reg.Publish("webclient.browser-1.heartbeat", []byte("")); err != nil {
		t.Fatalf("publish heartbeat: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if !a.Connected() {
		t.Fatalf("expected client to remain connected after heartbeat")
	}

	// disconnect
	if err := reg.Publish("webclient.browser-1.disconnect", []byte("")); err != nil {
		t.Fatalf("publish disconnect: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if a.Connected() {
		t.Fatalf("expected client to be disconnected")
	}
}

func TestWebClientTaskForwarding(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := NewWebClientActor("browser-1", []string{"webclient.browser-1.>"}, reg, NoopAuthenticator{})
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	// register via noop auth
	if err := reg.Publish("webclient.browser-1.register", []byte("{}")); err != nil {
		t.Fatalf("publish register: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// capture actor on incoming topic
	incoming := &recordingActor{id: "incoming-capture", topics: []string{"webclient.browser-1.incoming"}}
	if err := reg.Start(incoming); err != nil {
		t.Fatalf("start incoming: %v", err)
	}

	// task gets forwarded to incoming topic
	if err := reg.Publish("webclient.browser-1.task", []byte("do work")); err != nil {
		t.Fatalf("publish task: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return incoming.count() == 1 }) {
		t.Fatalf("expected task forwarded to incoming")
	}
	if string(incoming.lastData()) != "do work" {
		t.Fatalf("expected 'do work', got %q", string(incoming.lastData()))
	}

	// task dropped when not connected
	a.handleDisconnect()
	if err := reg.Publish("webclient.browser-1.task", []byte("do work 2")); err != nil {
		t.Fatalf("publish task: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if incoming.count() != 1 {
		t.Fatalf("expected task dropped when disconnected, still %d", incoming.count())
	}
}

func TestWebClientResultPublished(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	a := NewWebClientActor("browser-1", []string{"webclient.browser-1.>"}, reg, NoopAuthenticator{})
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	// capture actor on result topic
	result := &recordingActor{id: "result-capture", topics: []string{"webclient.browser-1.result"}}
	if err := reg.Start(result); err != nil {
		t.Fatalf("start result: %v", err)
	}

	// browser publishes result
	if err := reg.Publish("webclient.browser-1.result", []byte("task done")); err != nil {
		t.Fatalf("publish result: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return result.count() == 1 }) {
		t.Fatalf("expected result published")
	}
	if string(result.lastData()) != "task done" {
		t.Fatalf("expected 'task done', got %q", string(result.lastData()))
	}
}

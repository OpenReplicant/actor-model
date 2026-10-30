package kernel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// restCaptureActor captures messages on rest topics for assertions.
type restCaptureActor struct {
	id     string
	topics []string
	mu     sync.Mutex
	msgs   []Msg
}

func (a *restCaptureActor) ID() string       { return a.id }
func (a *restCaptureActor) Topics() []string { return a.topics }
func (a *restCaptureActor) Handle(_ context.Context, msg Msg) {
	a.mu.Lock()
	a.msgs = append(a.msgs, msg)
	a.mu.Unlock()
}

func (a *restCaptureActor) subjects() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.msgs))
	for i, m := range a.msgs {
		out[i] = m.Subject
	}
	return out
}

func TestRestActorGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	reg, cleanup := embeddedBus(t)
	defer cleanup()

	r := NewRestActor("rest-test", "GET", srv.URL, reg)
	if err := reg.Start(r); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &restCaptureActor{id: "cap", topics: []string{"rest.rest-test.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	if err := reg.Publish("rest.rest-test.request", []byte(`{"path":"/health"}`)); err != nil {
		t.Fatalf("publish request: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "rest.rest-test.responded")
	}) {
		t.Fatalf("expected responded message, got %v", cap.subjects())
	}

	var responded Msg
	for _, m := range cap.msgs {
		if m.Subject == "rest.rest-test.responded" {
			responded = m
			break
		}
	}
	if string(responded.Data) != `{"ok":true}` {
		t.Fatalf("expected response body, got %q", string(responded.Data))
	}
}

func TestRestActorRequestFailed(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	r := NewRestActor("rest-fail", "GET", "http://127.0.0.1:1/nonexistent", reg)
	if err := reg.Start(r); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &restCaptureActor{id: "cap2", topics: []string{"rest.rest-fail.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	if err := reg.Publish("rest.rest-fail.request", []byte(`{"path":"/x"}`)); err != nil {
		t.Fatalf("publish request: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "rest.rest-fail.failed")
	}) {
		t.Fatalf("expected failed message, got %v", cap.subjects())
	}
}

var _ = context.Background

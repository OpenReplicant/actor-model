package actors

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	kernel "actor-kernel/kernel"
)

func TestWebUIListActors(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	echo := NewEchoActor("echo", []string{"echo.>"}, reg)
	if err := reg.Start(echo); err != nil {
		t.Fatalf("start echo: %v", err)
	}

	ui := NewWebUIActor("web", []string{"webui.>"}, "", "", reg)
	if err := reg.Start(ui); err != nil {
		t.Fatalf("start webui: %v", err)
	}

	if err := reg.Publish("echo.test", []byte("ping")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Give the echo actor time to handle so it's registered.
	if !waitFor(t, 2*time.Second, func() bool {
		for _, a := range reg.ListActors() {
			if a.ID == "web" && len(a.Topics) > 0 {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("expected webui actor registered; actors=%v", reg.ListActors())
	}

	// Request the actor list via NATS request/reply.
	reply, err := reg.Request("webui.list.actors", []byte(""), 2*time.Second)
	if err != nil {
		t.Fatalf("request webui.list.actors: %v", err)
	}
	var actors []kernel.ActorInfo
	if err := json.Unmarshal(reply, &actors); err != nil {
		t.Fatalf("unmarshal actors: %v", err)
	}
	ids := map[string]bool{}
	for _, a := range actors {
		ids[a.ID] = true
	}
	if !ids["echo"] || !ids["web"] {
		t.Fatalf("expected echo and web in actor list, got %v", ids)
	}
}

// TestWebUIOwnersAndScheduler verifies the dashboard's two new read endpoints.
// They exist so the page can show who owns what and what the scheduler decided,
// which previously could only be inferred from the bus.
func TestWebUIOwnersAndScheduler(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	leases, err := kernel.NewLeaseStore(reg.Conn(), "webui-owners")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	reg.WithOwnership(leases, "node-x")

	echo := NewEchoActor("echo", []string{"echo.>"}, reg)
	if err := reg.Start(echo); err != nil {
		t.Fatalf("start echo: %v", err)
	}

	ui := NewWebUIActor("web", []string{"webui.>"}, "", "", reg)
	if err := reg.Start(ui); err != nil {
		t.Fatalf("start webui: %v", err)
	}

	reply, err := reg.Request("webui.owners", []byte(""), 2*time.Second)
	if err != nil {
		t.Fatalf("request webui.owners: %v", err)
	}
	var owners struct {
		Enabled bool           `json:"enabled"`
		Node    string         `json:"node"`
		Leases  []kernel.Lease `json:"leases"`
	}
	if err := json.Unmarshal(reply, &owners); err != nil {
		t.Fatalf("unmarshal owners: %v", err)
	}
	if !owners.Enabled || owners.Node != "node-x" {
		t.Fatalf("ownership not reported: %+v", owners)
	}
	found := false
	for _, l := range owners.Leases {
		if l.Actor == "echo" && l.Node == "node-x" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected echo lease owned by node-x, got %+v", owners.Leases)
	}

	// Scheduler state: empty until a scheduler is registered.
	reply, err = reg.Request("webui.scheduler", []byte(""), 2*time.Second)
	if err != nil {
		t.Fatalf("request webui.scheduler: %v", err)
	}
	if string(reply) != "[]" {
		t.Fatalf("expected empty scheduler list, got %s", reply)
	}
}

// TestWebUIStartServesHTTPImmediately is a regression test for a real bug: the
// HTTP listener used to start lazily on the first message, so a dashboard
// nobody published to never came up at all.
func TestWebUIStartServesHTTPImmediately(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	ui := NewWebUIActor("web", []string{"webui.>"}, "127.0.0.1:0", "ws://example", reg)
	if err := reg.Start(ui); err != nil {
		t.Fatalf("start webui: %v", err)
	}
	defer reg.Stop(ui)

	// Start must have opened the listener without any message being published.
	ui.mu.Lock()
	srv := ui.srv
	ui.mu.Unlock()
	if srv == nil {
		t.Fatal("webui did not open its listener on Start (lazy-start bug)")
	}

	// And Stop must close it.
	if err := reg.Stop(ui); err != nil {
		t.Fatalf("stop webui: %v", err)
	}
	ui.mu.Lock()
	closed := ui.srv == nil
	ui.mu.Unlock()
	if !closed {
		t.Fatal("webui listener was not released on Stop")
	}
}

// TestWebUIDashboardHasNoUnsubstitutedPlaceholders: a template placeholder that
// survives rendering breaks the page silently (an undefined variable in JS).
func TestWebUIDashboardHasNoUnsubstitutedPlaceholders(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	ui := NewWebUIActor("web", []string{"webui.>"}, "", "ws://host:1234", reg)
	ui.WithNodeID("node-z").WithBrowserAuth(kernel.AuthConfig{Method: kernel.AuthUserPass, Username: "u", Password: "p"})
	if err := reg.Start(ui); err != nil {
		t.Fatalf("start: %v", err)
	}

	var buf strings.Builder
	rec := &fakeResponseWriter{buf: &buf}
	ui.handleDashboard(rec, nil)
	page := buf.String()

	for _, want := range []string{"ws://host:1234", "node-z", `"user":"u"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("rendered page missing %q", want)
		}
	}
}

// fakeResponseWriter is a minimal http.ResponseWriter capturing the body.
type fakeResponseWriter struct {
	buf *strings.Builder
}

func (f *fakeResponseWriter) Header() http.Header         { return http.Header{} }
func (f *fakeResponseWriter) Write(b []byte) (int, error) { return f.buf.Write(b) }
func (f *fakeResponseWriter) WriteHeader(int)             {}

func TestWebUIMetrics(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	ui := NewWebUIActor("web", []string{"webui.>"}, "", "", reg)
	if err := reg.Start(ui); err != nil {
		t.Fatalf("start webui: %v", err)
	}

	reply, err := reg.Request("webui.metrics", []byte(""), 2*time.Second)
	if err != nil {
		t.Fatalf("request webui.metrics: %v", err)
	}
	if len(reply) == 0 {
		t.Fatalf("expected metrics text, got empty")
	}
}

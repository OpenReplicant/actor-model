// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"encoding/json"
	"testing"
	"time"
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
	var actors []ActorInfo
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

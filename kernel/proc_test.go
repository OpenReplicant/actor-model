package kernel

import (
	"testing"
	"time"
)

func TestProcActorStartStop(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	p := NewProcActor("proc-test", "sleep", []string{"100"}, reg)
	if err := reg.Start(p); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &procCaptureActor{id: "cap", topics: []string{"proc.proc-test.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	if err := reg.Publish("proc.proc-test.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "proc.proc-test.started")
	}) {
		t.Fatalf("expected started message, got %v", cap.subjects())
	}

	if err := reg.Publish("proc.proc-test.stop", []byte("")); err != nil {
		t.Fatalf("publish stop: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "proc.proc-test.stopped")
	}) {
		t.Fatalf("expected stopped message, got %v", cap.subjects())
	}
}

func TestProcActorStartWhenAlreadyRunning(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	p := NewProcActor("proc-test2", "sleep", []string{"100"}, reg)
	if err := reg.Start(p); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &procCaptureActor{id: "cap2", topics: []string{"proc.proc-test2.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	if err := reg.Publish("proc.proc-test2.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "proc.proc-test2.started")
	}) {
		t.Fatalf("expected started message, got %v", cap.subjects())
	}

	// Start again while running -> start-failed
	if err := reg.Publish("proc.proc-test2.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "proc.proc-test2.start-failed")
	}) {
		t.Fatalf("expected start-failed message, got %v", cap.subjects())
	}
}

func TestProcActorStopWhenNotRunning(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	p := NewProcActor("proc-test3", "true", nil, reg)
	if err := reg.Start(p); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &procCaptureActor{id: "cap3", topics: []string{"proc.proc-test3.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Stop when never started -> no error, no stopped message.
	if err := reg.Publish("proc.proc-test3.stop", []byte("")); err != nil {
		t.Fatalf("publish stop: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	for _, s := range cap.subjects() {
		if s == "proc.proc-test3.stopped" {
			t.Fatalf("expected no stopped message for stop-not-running, got %v", cap.subjects())
		}
	}
}

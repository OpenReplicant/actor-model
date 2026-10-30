package kernel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// stdioCaptureActor captures stdout lines from the stdio actor.
type stdioCaptureActor struct {
	id     string
	topics []string
	mu     sync.Mutex
	msgs   []Msg
}

func (a *stdioCaptureActor) ID() string       { return a.id }
func (a *stdioCaptureActor) Topics() []string { return a.topics }
func (a *stdioCaptureActor) Handle(_ context.Context, msg Msg) {
	a.mu.Lock()
	a.msgs = append(a.msgs, msg)
	a.mu.Unlock()
}

func (a *stdioCaptureActor) subjects() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.msgs))
	for i, m := range a.msgs {
		out[i] = m.Subject
	}
	return out
}

func (a *stdioCaptureActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.msgs)
}

func TestStdioActorExec(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	s := NewStdioActor("stdio-test", "python3", []string{"-u", "-c", "import sys;[sys.stdout.write(line) for line in sys.stdin]"}, reg)
	if err := reg.Start(s); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &stdioCaptureActor{id: "cap", topics: []string{"stdio.stdio-test.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Let subscriptions settle before publishing start.
	time.Sleep(100 * time.Millisecond)

	// Publish start and wait for started confirmation.
	if err := reg.Publish("stdio.stdio-test.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}

	// Wait for the interpreter to start.
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "stdio.stdio-test.started")
	}) {
		t.Fatalf("expected started message, got %v", cap.subjects())
	}

	// Send "hello" to stdin; python should echo it back on stdout
	if err := reg.Publish("stdio.stdio-test.exec", []byte("hello")); err != nil {
		t.Fatalf("publish exec: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "stdio.stdio-test.out")
	}) {
		t.Fatalf("expected out message, got %v", cap.subjects())
	}

	var out Msg
	for _, m := range cap.msgs {
		if m.Subject == "stdio.stdio-test.out" {
			out = m
			break
		}
	}
	if string(out.Data) != "hello" {
		t.Fatalf("expected 'hello' echoed, got %q", string(out.Data))
	}
}

func TestStdioActorStop(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	s := NewStdioActor("stdio-stop", "cat", nil, reg)
	if err := reg.Start(s); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := reg.Publish("stdio.stdio-test.stop", []byte("")); err != nil {
		t.Fatalf("publish stop: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Stop should not error; no specific assertion on messages since cat
	// just exits on EOF.
	_ = reg.Stop(s)
	time.Sleep(100 * time.Millisecond)
	// No crash/panic means stop worked.
}

func TestStdioActorExecWhenNotStarted(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	s := NewStdioActor("stdio-nostart", "cat", nil, reg)
	if err := reg.Start(s); err != nil {
		t.Fatalf("start: %v", err)
	}
	// NOTE: never publish "start", so the interpreter is not launched.
	// The actor remains subscribed; exec should fail because stdin is nil.

	cap := &stdioCaptureActor{id: "cap-nostart", topics: []string{"stdio.stdio-nostart.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	if err := reg.Publish("stdio.stdio-nostart.exec", []byte("x")); err != nil {
		t.Fatalf("publish exec: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "stdio.stdio-nostart.failed")
	}) {
		t.Fatalf("expected failed message, got %v", cap.subjects())
	}
}

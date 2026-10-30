package kernel

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// embeddedBus starts an in-process NATS server and returns a Registry connected
// to it, plus a cleanup function. All clients are goroutines in this process.
func embeddedBus(t *testing.T) (*Registry, func()) {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // let the OS pick an ephemeral port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,        // enable JetStream for durability tests
		StoreDir:  t.TempDir(), // isolated store: no stream leakage between tests/runs
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats server not ready")
	}

	nc, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("connect to nats: %v", err)
	}

	reg := NewRegistry(nc)
	return reg, func() {
		reg.Close()
		nc.Close()
		srv.Shutdown()
	}
}

// generateUserNKey creates a user NKey pair and writes the seed to a temp file,
// returning the seed path and the public key.
func generateUserNKey(t *testing.T) (seedFile, publicKey string) {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create user nkey: %v", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatalf("nkey seed: %v", err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatalf("nkey public key: %v", err)
	}
	path := t.TempDir() + "/user.nk"
	if err := os.WriteFile(path, seed, 0600); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	return path, pub
}

// recordingActor captures every message it receives in order.
type recordingActor struct {
	id      string
	topics  []string
	mu      sync.Mutex
	msgs    []Msg
	handles int
}

func (a *recordingActor) ID() string       { return a.id }
func (a *recordingActor) Topics() []string { return a.topics }
func (a *recordingActor) Handle(ctx context.Context, msg Msg) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.msgs = append(a.msgs, msg)
	a.handles++
}
func (a *recordingActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.handles
}
func (a *recordingActor) lastSubject() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.msgs) == 0 {
		return ""
	}
	return a.msgs[len(a.msgs)-1].Subject
}
func (a *recordingActor) lastData() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.msgs) == 0 {
		return nil
	}
	return a.msgs[len(a.msgs)-1].Data
}

func (a *recordingActor) subjects() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.msgs))
	for i, m := range a.msgs {
		out[i] = m.Subject
	}
	return out
}

// waitFor blocks until cond is true or deadline passes, returning whether cond held.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// contains reports whether target appears in subjects.
func contains(subjects []string, target string) bool {
	for _, s := range subjects {
		if s == target {
			return true
		}
	}
	return false
}

// procCaptureActor captures messages on proc/docker topics for assertions.
type procCaptureActor struct {
	id     string
	topics []string
	mu     sync.Mutex
	msgs   []Msg
}

func (a *procCaptureActor) ID() string       { return a.id }
func (a *procCaptureActor) Topics() []string { return a.topics }
func (a *procCaptureActor) Handle(_ context.Context, msg Msg) {
	a.mu.Lock()
	a.msgs = append(a.msgs, msg)
	a.mu.Unlock()
}
func (a *procCaptureActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.msgs)
}
func (a *procCaptureActor) subjects() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.msgs))
	for i, m := range a.msgs {
		out[i] = m.Subject
	}
	return out
}

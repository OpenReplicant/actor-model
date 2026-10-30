package kernel

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// dockerShimDir creates a temp dir containing a fake "docker" executable so
// tests need no Docker daemon or pulled images.
func dockerShimDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	shim := filepath.Join(dir, "docker")
	script := `#!/bin/bash
set -e
STATE_DIR="__STATE_DIR__"
mkdir -p "$STATE_DIR"
STATE="$STATE_DIR/state"
case "$1" in
  run)
    # If already running, docker returns conflict (exit 1)
    if [ -f "$STATE" ]; then
      echo "Conflict: container name already in use" >&2
      exit 1
    fi
    echo "fake-cid"
    echo "running" > "$STATE"
    ;;
  stop)
    rm -f "$STATE"
    ;;
  rm)
    ;;
  *)
    echo "unknown: $@" >&2
    exit 1
    ;;
esac
`
	script = strings.ReplaceAll(script, "__STATE_DIR__", dir)
	if err := os.WriteFile(shim, []byte(script), 0755); err != nil {
		t.Fatalf("write shim: %v", err)
	}
	return dir
}

func TestDockerActorStartStop(t *testing.T) {
	shimDir := dockerShimDir(t)
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	d := NewDockerActor("docker-test", filepath.Join(shimDir, "docker"), "nginx", reg)
	if err := reg.Start(d); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &procCaptureActor{id: "cap", topics: []string{"docker.docker-test.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Let subscriptions settle before publishing start.
	time.Sleep(100 * time.Millisecond)

	if err := reg.Publish("docker.docker-test.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "docker.docker-test.started")
	}) {
		t.Fatalf("expected started message, got %v", cap.subjects())
	}

	if err := reg.Publish("docker.docker-test.stop", []byte("")); err != nil {
		t.Fatalf("publish stop: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "docker.docker-test.stopped")
	}) {
		t.Fatalf("expected stopped message, got %v", cap.subjects())
	}
}

func TestDockerActorStartWhenRunning(t *testing.T) {
	shimDir := dockerShimDir(t)
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	d := NewDockerActor("docker-test2", filepath.Join(shimDir, "docker"), "nginx", reg)
	if err := reg.Start(d); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &procCaptureActor{id: "cap2", topics: []string{"docker.docker-test2.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Let subscriptions settle before publishing start.
	time.Sleep(100 * time.Millisecond)

	if err := reg.Publish("docker.docker-test2.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "docker.docker-test2.started")
	}) {
		t.Fatalf("expected started message, got %v", cap.subjects())
	}

	// Start again while running -> start-failed
	if err := reg.Publish("docker.docker-test2.start", []byte("")); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return contains(cap.subjects(), "docker.docker-test2.start-failed")
	}) {
		t.Fatalf("expected start-failed message, got %v", cap.subjects())
	}
}

func TestDockerActorStopWhenNotRunning(t *testing.T) {
	shimDir := dockerShimDir(t)
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	d := NewDockerActor("docker-test3", filepath.Join(shimDir, "docker"), "true", reg)
	if err := reg.Start(d); err != nil {
		t.Fatalf("start: %v", err)
	}

	cap := &procCaptureActor{id: "cap3", topics: []string{"docker.docker-test3.>"}}
	if err := reg.Start(cap); err != nil {
		t.Fatalf("start cap: %v", err)
	}

	// Stop when never started -> no stopped message
	if err := reg.Publish("docker.docker-test3.stop", []byte("")); err != nil {
		t.Fatalf("publish stop: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	for _, s := range cap.subjects() {
		if s == "docker.docker-test3.stopped" {
			t.Fatalf("expected no stopped message for stop-not-running, got %v", cap.subjects())
		}
	}
}

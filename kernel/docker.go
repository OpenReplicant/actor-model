// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"os/exec"
	"sync"
)

// DockerActor wraps a single Docker container as a bus-controlled actor.
// Subscribes to docker.<id>.>:
//   - start: docker run -d --name <name> , publish docker.<id>.started
//   - stop: docker stop <name>, publish docker.<id>.stopped
//   - start-failed: published when start is requested while already running
//     or when docker cannot launch the container.
//
// Unlike ProcActor, the exec.Command here is the `docker run` CLI which exits
// immediately; the container lives on in the Docker daemon. running reflects
// whether we believe the container is up, set by start/stop.
type DockerActor struct {
	id    string
	bin   string // path to the docker executable
	image string
	name  string
	reg   Publisher

	mu      sync.Mutex
	running bool
}

// NewDockerActor creates a docker actor controlling the given image.
// Arguments follow the proc convention: (id, bin, image).
func NewDockerActor(id string, bin string, image string, reg Publisher) *DockerActor {
	return &DockerActor{
		id:    id,
		bin:   bin,
		image: image,
		name:  "actor-" + id,
		reg:   reg,
	}
}

func (a *DockerActor) ID() string       { return a.id }
func (a *DockerActor) Topics() []string { return []string{"docker." + a.id + ".>"} }

func (a *DockerActor) Handle(ctx context.Context, msg Msg) {
	switch msg.Subject {
	case "docker." + a.id + ".start":
		a.handleStart()
	case "docker." + a.id + ".stop":
		a.handleStop()
	}
}

func (a *DockerActor) handleStart() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		a.reg.Publish("docker."+a.id+".start-failed", []byte("already running"))
		return
	}
	c := exec.Command(a.bin, "run", "-d", "--name", a.name, a.image)
	if err := c.Start(); err != nil {
		a.mu.Unlock()
		a.reg.Publish("docker."+a.id+".start-failed", []byte(err.Error()))
		return
	}
	a.running = true
	a.mu.Unlock()

	a.reg.Publish("docker."+a.id+".started", []byte(""))
}

func (a *DockerActor) handleStop() {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return // nothing running; no stopped message
	}
	a.running = false
	a.mu.Unlock()

	stop := exec.Command(a.bin, "stop", a.name)
	_ = stop.Run()

	a.reg.Publish("docker."+a.id+".stopped", []byte(""))
}

// ensure DockerActor implements Actor at compile time.
var _ Actor = (*DockerActor)(nil)

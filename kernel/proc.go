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

// ProcActor wraps a single OS process as a bus-controlled actor.
// Subscribes to proc.<id>.>:
//   - start: exec.Command the configured binary, publish proc.<id>.started
//   - stop: kill the process, publish proc.<id>.stopped
//   - start-failed: published when start is requested while already running
//     or when the binary cannot be launched.
type ProcActor struct {
	id   string
	cmd  string
	args []string
	reg  Publisher

	mu      sync.Mutex
	c       *exec.Cmd     // the running process, if any
	done    chan struct{} // closed by reap goroutine when the process exits
	running bool
}

// NewProcActor creates a process actor that controls the given binary.
func NewProcActor(id string, cmd string, args []string, reg Publisher) *ProcActor {
	return &ProcActor{
		id:   id,
		cmd:  cmd,
		args: args,
		reg:  reg,
	}
}

func (a *ProcActor) ID() string       { return a.id }
func (a *ProcActor) Topics() []string { return []string{"proc." + a.id + ".>"} }

func (a *ProcActor) Handle(ctx context.Context, msg Msg) {
	switch msg.Subject {
	case "proc." + a.id + ".start":
		a.handleStart()
	case "proc." + a.id + ".stop":
		a.handleStop()
	}
}

func (a *ProcActor) handleStart() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		a.reg.Publish("proc."+a.id+".start-failed", []byte("already running"))
		return
	}
	c := exec.Command(a.cmd, a.args...)
	if err := c.Start(); err != nil {
		a.mu.Unlock()
		a.reg.Publish("proc."+a.id+".start-failed", []byte(err.Error()))
		return
	}
	a.c = c
	a.done = make(chan struct{})
	a.running = true
	a.mu.Unlock()

	// Reap the process in the background; only this goroutine calls c.Wait(),
	// so handleStop must not call Wait. It closes done when the process exits.
	go a.reap(c)
	a.reg.Publish("proc."+a.id+".started", []byte(""))
}

func (a *ProcActor) reap(c *exec.Cmd) {
	_ = c.Wait()
	a.mu.Lock()
	close(a.done)
	if a.running && a.c == c {
		a.running = false
		a.c = nil
	}
	a.mu.Unlock()
}

func (a *ProcActor) handleStop() {
	a.mu.Lock()
	c := a.c
	done := a.done
	a.c = nil
	a.running = false
	// leave done intact so the reap goroutine can close it
	a.mu.Unlock()

	if c != nil {
		_ = c.Process.Kill()
		<-done // wait for the reaper to finish before publishing stopped
		a.reg.Publish("proc."+a.id+".stopped", []byte(""))
	}
}

// ensure ProcActor implements Actor at compile time.
var _ Actor = (*ProcActor)(nil)

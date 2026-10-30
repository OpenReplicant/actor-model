package actors

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"sync"

	kernel "actor-kernel/kernel"
)

// StdioActor wraps an external interpreter as a persistent STDIO process, so
// actors can be written in any language. Subscribes to stdio.<id>.>:
//   - start: launch the interpreter; a goroutine reads stdout and publishes
//     stdio.<id>.out for each line
//   - exec: send message body to the interpreter's stdin
//   - stop: kill the interpreter
//
// The interpreter stays alive across messages; state persists in the
// interpreter. This is the STDIO plugin protocol: the plugin reads JSON Msg
// from stdin and writes JSON Msg to stdout.
type StdioActor struct {
	id   string
	cmd  string
	args []string
	reg  kernel.Publisher

	mu      sync.Mutex
	cmdProc *exec.Cmd // the running interpreter process
	stdin   io.WriteCloser
	running bool
	done    chan struct{} // closed by the stdout reader goroutine when stdin closes
}

// NewStdioActor creates a stdio actor wrapping the given interpreter.
func NewStdioActor(id string, cmd string, args []string, reg kernel.Publisher) *StdioActor {
	return &StdioActor{
		id:   id,
		cmd:  cmd,
		args: args,
		reg:  reg,
	}
}

func (a *StdioActor) ID() string       { return a.id }
func (a *StdioActor) Topics() []string { return []string{"stdio." + a.id + ".>"} }

func (a *StdioActor) Handle(ctx context.Context, msg kernel.Msg) {
	switch msg.Subject {
	case "stdio." + a.id + ".start":
		a.handleStart()
	case "stdio." + a.id + ".exec":
		a.handleExec(msg.Data)
	case "stdio." + a.id + ".stop":
		a.handleStop()
	}
}

func (a *StdioActor) handleStart() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		a.reg.Publish("stdio."+a.id+".start-failed", []byte("already running"))
		return
	}
	c := exec.Command(a.cmd, a.args...)
	stdin, err := c.StdinPipe()
	if err != nil {
		a.mu.Unlock()
		a.reg.Publish("stdio."+a.id+".start-failed", []byte(err.Error()))
		return
	}
	stdout, err := c.StdoutPipe()
	if err != nil {
		a.mu.Unlock()
		a.reg.Publish("stdio."+a.id+".start-failed", []byte(err.Error()))
		return
	}
	if err := c.Start(); err != nil {
		a.mu.Unlock()
		a.reg.Publish("stdio."+a.id+".start-failed", []byte(err.Error()))
		return
	}
	a.cmdProc = c
	a.stdin = stdin
	a.running = true
	a.done = make(chan struct{})
	a.mu.Unlock()

	go a.readStdout(stdout)
	a.reg.Publish("stdio."+a.id+".started", []byte(""))
}

func (a *StdioActor) readStdout(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Bytes()
		a.reg.Publish("stdio."+a.id+".out", line)
	}
	a.mu.Lock()
	a.running = false
	a.cmdProc = nil
	a.stdin = nil
	close(a.done)
	a.mu.Unlock()
}

func (a *StdioActor) handleExec(data []byte) {
	a.mu.Lock()
	stdin := a.stdin
	a.mu.Unlock()

	if stdin == nil {
		a.reg.Publish("stdio."+a.id+".failed", []byte("not started"))
		return
	}
	if _, err := stdin.Write(data); err != nil {
		a.reg.Publish("stdio."+a.id+".failed", []byte("write error: "+err.Error()))
		return
	}
	if _, err := stdin.Write([]byte("\n")); err != nil {
		a.reg.Publish("stdio."+a.id+".failed", []byte("write error: "+err.Error()))
	}
}

func (a *StdioActor) handleStop() {
	a.mu.Lock()
	stdin := a.stdin
	a.stdin = nil
	a.running = false
	a.mu.Unlock()

	if stdin != nil {
		stdin.Close()
		<-a.done
	}

	a.reg.Publish("stdio."+a.id+".stopped", []byte(""))
}

// ensure StdioActor implements Actor at compile time.
var _ kernel.Actor = (*StdioActor)(nil)

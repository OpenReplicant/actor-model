// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"sync"
)

// MCPClientActor wraps an external MCP server as an actor on the bus — the
// same way RestActor wraps an HTTP endpoint. It starts the MCP server as a
// subprocess (stdio transport) and speaks JSON-RPC 2.0 + MCP to it, exposing
// the server's tools as actor topics.
//
// Subscribes to mcpclient.<id>.>:
//   - mcpclient.<id>.start   → start the MCP server subprocess + initialize
//   - mcpclient.<id>.stop    → stop the MCP server subprocess
//   - mcpclient.<id>.call    → forward an MCP tools/call to the server
//     (body: {"name":..., "arguments":...})
//     replies on msg.Reply with the tool result
//   - mcpclient.<id>.list    → ask the server for tools/list
//     replies on msg.Reply with the tool list
//
// Publish mcpclient.<id>.initialized once the MCP server is initialized.
type MCPClientActor struct {
	id     string
	topics []string
	reg    Publisher

	mu      sync.Mutex
	cmd     *exec.Cmd // the MCP server subprocess
	stdin   io.WriteCloser
	scanner *bufio.Scanner
	nextID  int
	running bool
}

// NewMCPClientActor creates an MCP client actor that wraps the given MCP
// server command (e.g. "npx", args: ["-y", "@modelcontextprotocol/server-..."]).
func NewMCPClientActor(id string, cmd string, args []string, reg Publisher) *MCPClientActor {
	return &MCPClientActor{
		id:     id,
		topics: []string{"mcpclient." + id + ".>"},
		reg:    reg,
		cmd:    exec.Command(cmd, args...),
	}
}

func (a *MCPClientActor) ID() string       { return a.id }
func (a *MCPClientActor) Topics() []string { return a.topics }

func (a *MCPClientActor) Handle(ctx context.Context, msg Msg) {
	switch msg.Subject {
	case "mcpclient." + a.id + ".start":
		a.handleStart()
	case "mcpclient." + a.id + ".stop":
		a.handleStop()
	case "mcpclient." + a.id + ".call":
		a.handleCall(ctx, msg)
	case "mcpclient." + a.id + ".list":
		a.handleList(ctx, msg)
	}
}

func (a *MCPClientActor) handleStart() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		a.reg.Publish("mcpclient."+a.id+".initialized", []byte("already running"))
		return
	}
	c := a.cmd
	stdin, err := c.StdinPipe()
	if err != nil {
		a.mu.Unlock()
		a.reg.Publish("mcpclient."+a.id+".error", []byte("stdin error: "+err.Error()))
		return
	}
	stdout, err := c.StdoutPipe()
	if err != nil {
		a.mu.Unlock()
		a.reg.Publish("mcpclient."+a.id+".error", []byte("stdout error: "+err.Error()))
		return
	}
	if err := c.Start(); err != nil {
		a.mu.Unlock()
		a.reg.Publish("mcpclient."+a.id+".error", []byte("start error: "+err.Error()))
		return
	}
	a.stdin = stdin
	a.scanner = bufio.NewScanner(stdout)
	a.running = true
	a.mu.Unlock()

	go a.readLoop(stdout)

	// Send MCP initialize.
	initReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      0,
		"method":  "initialize",
		"params": map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]interface{}{},
		},
	}
	b, _ := json.Marshal(initReq)
	if _, err := stdin.Write(append(b, '\n')); err != nil {
		a.reg.Publish("mcpclient."+a.id+".error", []byte("init write error: "+err.Error()))
		return
	}
	a.readResponse() // consume initialize result

	a.reg.Publish("mcpclient."+a.id+".initialized", []byte(""))
}

func (a *MCPClientActor) handleStop() {
	a.mu.Lock()
	stdin := a.stdin
	a.stdin = nil
	a.running = false
	a.mu.Unlock()
	if stdin != nil {
		stdin.Close()
	}
	if a.cmd != nil && a.cmd.Process != nil {
		_ = a.cmd.Process.Kill()
	}
}

func (a *MCPClientActor) handleCall(ctx context.Context, msg Msg) {
	var req struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.reg.Publish("mcpclient."+a.id+".error", []byte("invalid call: "+err.Error()))
		return
	}

	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		a.reg.Publish("mcpclient."+a.id+".error", []byte("mcp client not running"))
		return
	}
	stdin := a.stdin
	id := a.nextID
	a.nextID++
	a.mu.Unlock()

	if stdin == nil {
		a.reg.Publish("mcpclient."+a.id+".error", []byte("mcp client stdin unavailable"))
		return
	}

	call := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      req.Name,
			"arguments": req.Arguments,
		},
	}
	b, _ := json.Marshal(call)
	if _, err := stdin.Write(append(b, '\n')); err != nil {
		a.reg.Publish("mcpclient."+a.id+".error", []byte("write error: "+err.Error()))
		return
	}

	resp := a.readResponse()
	if msg.Reply != "" && resp != nil {
		_ = a.reg.Publish(msg.Reply, resp)
	}
}

func (a *MCPClientActor) handleList(ctx context.Context, msg Msg) {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		a.reg.Publish("mcpclient."+a.id+".error", []byte("mcp client not running"))
		return
	}
	stdin := a.stdin
	id := a.nextID
	a.nextID++
	a.mu.Unlock()

	list := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "tools/list",
		"params":  map[string]interface{}{},
	}
	b, _ := json.Marshal(list)
	if _, err := stdin.Write(append(b, '\n')); err != nil {
		a.reg.Publish("mcpclient."+a.id+".error", []byte("write error: "+err.Error()))
		return
	}

	resp := a.readResponse()
	if msg.Reply != "" && resp != nil {
		_ = a.reg.Publish(msg.Reply, resp)
	}
}

// readResponse reads one JSON-RPC response line from the MCP server stdout.
func (a *MCPClientActor) readResponse() []byte {
	a.mu.Lock()
	scanner := a.scanner
	a.mu.Unlock()
	if scanner == nil {
		return nil
	}
	if scanner.Scan() {
		return scanner.Bytes()
	}
	return nil
}

// readLoop drains stdout so the subprocess doesn't block on its output pipe.
func (a *MCPClientActor) readLoop(stdout io.Reader) {
	io.Copy(io.Discard, stdout)
}

// ensure MCPClientActor implements Actor at compile time.
var _ Actor = (*MCPClientActor)(nil)

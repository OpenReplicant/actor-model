// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// MCP protocol constants.
const (
	MCPProtocolVersion = "2024-11-05"
	MCPServerName      = "actor-kernel"
	MCPServerVersion   = "0.1.0"
)

// JSON-RPC 2.0 types.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

const (
	jsonRPCParseError     = -32700
	jsonRPCInvalidRequest = -32600
	jsonRPCMethodNotFound = -32601
	jsonRPCInvalidParams  = -32602
	jsonRPCInternalError  = -32603
)

// MCPServerActor exposes the actor kernel as an MCP server over NATS.
// It subscribes to mcp.<id>.> and speaks JSON-RPC 2.0 + the Model Context
// Protocol, so LLM agents and MCP clients can discover and control actors:
//
//   - mcp.<id>.request   → JSON-RPC request from an MCP client
//
// It publishes responses on msg.Reply:
//
//   - initialize       → server capabilities + protocol version
//   - tools/list       → actors as MCP tools (one per actor + kernel actions)
//   - tools/call       → invoke an actor method or kernel action
//   - resources/list   → actor registry, metrics, IDL as MCP resources
//   - resources/read   → read a specific resource (actor state, metrics)
//
// MCPClientActor (in mcp_client.go) is the mirror: it wraps an external MCP
// server as an actor on the bus, the same way RestActor wraps an HTTP endpoint.
type MCPServerActor struct {
	id     string
	topics []string
	reg    *Registry

	mu sync.Mutex
}

// NewMCPServerActor creates an MCP server actor on the given registry.
func NewMCPServerActor(id string, topics []string, reg *Registry) *MCPServerActor {
	return &MCPServerActor{id: id, topics: topics, reg: reg}
}

func (a *MCPServerActor) ID() string       { return a.id }
func (a *MCPServerActor) Topics() []string { return a.topics }

func (a *MCPServerActor) Handle(ctx context.Context, msg Msg) {
	if msg.Reply == "" {
		// MCP requires a reply channel for request/response.
		return
	}

	var req jsonRPCRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.sendError(msg.Reply, nil, jsonRPCParseError, "parse error", err)
		return
	}

	if req.JSONRPC != "2.0" {
		a.sendError(msg.Reply, req.ID, jsonRPCInvalidRequest, "invalid request", nil)
		return
	}

	switch req.Method {
	case "initialize":
		a.handleInitialize(msg.Reply, req.ID, req.Params)
	case "tools/list":
		a.handleToolsList(msg.Reply, req.ID)
	case "tools/call":
		a.handleToolsCall(msg.Reply, req.ID, req.Params)
	case "resources/list":
		a.handleResourcesList(msg.Reply, req.ID)
	case "resources/read":
		a.handleResourcesRead(msg.Reply, req.ID, req.Params)
	case "ping":
		a.sendResult(msg.Reply, req.ID, map[string]string{})
	default:
		a.sendError(msg.Reply, req.ID, jsonRPCMethodNotFound, "method not found", nil)
	}
}

func (a *MCPServerActor) handleInitialize(reply string, id json.RawMessage, params json.RawMessage) {
	result := map[string]interface{}{
		"protocolVersion": MCPProtocolVersion,
		"serverInfo": map[string]string{
			"name":    MCPServerName,
			"version": MCPServerVersion,
		},
		"capabilities": map[string]interface{}{
			"tools":     map[string]bool{},
			"resources": map[string]bool{},
		},
	}
	a.sendResult(reply, id, result)
}

func (a *MCPServerActor) handleToolsList(reply string, id json.RawMessage) {
	idl := a.reg.IDRegistry()
	tools := make([]map[string]interface{}, 0, len(idl.Actors))
	for _, spec := range idl.Actors {
		for _, m := range spec.Methods {
			tools = append(tools, map[string]interface{}{
				"name":        fmt.Sprintf("%s.%s", spec.ID, m.Name),
				"description": m.Description,
				"inputSchema": m.InputSchema,
			})
		}
		// Kernel-level actions per actor.
		tools = append(tools, map[string]interface{}{
			"name":        fmt.Sprintf("%s.stop", spec.ID),
			"description": fmt.Sprintf("Stop actor %s", spec.ID),
			"inputSchema": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		})
	}
	// Registry-level tools.
	tools = append(tools, map[string]interface{}{
		"name":        "actors.list",
		"description": "List all registered actors",
		"inputSchema": map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{},
		},
	})
	tools = append(tools, map[string]interface{}{
		"name":        "idl.get",
		"description": "Get the standard IDL for all actors",
		"inputSchema": map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{},
		},
	})

	a.sendResult(reply, id, map[string]interface{}{
		"tools": tools,
	})
}

func (a *MCPServerActor) handleToolsCall(reply string, id json.RawMessage, params json.RawMessage) {
	var args struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments,omitempty"`
	}
	if err := json.Unmarshal(params, &args); err != nil {
		a.sendError(reply, id, jsonRPCInvalidParams, "invalid params", err)
		return
	}

	switch {
	case args.Name == "actors.list":
		actors := a.reg.ListActors()
		a.sendResult(reply, id, map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"type": "text",
					"text": mustJSON(actors),
				},
			},
		})
	case args.Name == "idl.get":
		idl := a.reg.IDRegistry()
		a.sendResult(reply, id, map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"type": "text",
					"text": mustJSON(idl),
				},
			},
		})
	default:
		// "<actor>.<method>" or "<actor>.stop"
		a.sendError(reply, id, jsonRPCMethodNotFound, "tool not implemented: "+args.Name, nil)
	}
}

func (a *MCPServerActor) handleResourcesList(reply string, id json.RawMessage) {
	resources := []map[string]interface{}{
		{
			"uri":         "actor-kernel://actors",
			"name":        "Actors",
			"description": "List of all registered actors",
			"mimeType":    "application/json",
		},
		{
			"uri":         "actor-kernel://idl",
			"name":        "IDL",
			"description": "Standard IDL for all actors",
			"mimeType":    "application/json",
		},
		{
			"uri":         "actor-kernel://metrics",
			"name":        "Metrics",
			"description": "Prometheus text-format metrics",
			"mimeType":    "text/plain",
		},
		{
			"uri":         "actor-kernel://owners",
			"name":        "Owners",
			"description": "Cluster-wide actor ownership leases (which node owns which actor id)",
			"mimeType":    "application/json",
		},
	}
	a.sendResult(reply, id, map[string]interface{}{
		"resources": resources,
	})
}

func (a *MCPServerActor) handleResourcesRead(reply string, id json.RawMessage, params json.RawMessage) {
	var args struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(params, &args); err != nil {
		a.sendError(reply, id, jsonRPCInvalidParams, "invalid params", err)
		return
	}

	switch args.URI {
	case "actor-kernel://actors":
		a.sendResult(reply, id, map[string]interface{}{
			"contents": []map[string]interface{}{
				{
					"uri":      args.URI,
					"mimeType": "application/json",
					"text":     mustJSON(a.reg.ListActors()),
				},
			},
		})
	case "actor-kernel://idl":
		a.sendResult(reply, id, map[string]interface{}{
			"contents": []map[string]interface{}{
				{
					"uri":      args.URI,
					"mimeType": "application/json",
					"text":     mustJSON(a.reg.IDRegistry()),
				},
			},
		})
	case "actor-kernel://metrics":
		a.sendResult(reply, id, map[string]interface{}{
			"contents": []map[string]interface{}{
				{
					"uri":      args.URI,
					"mimeType": "text/plain",
					"text":     a.reg.Metrics().TextFormat(),
				},
			},
		})
	case "actor-kernel://owners":
		// Who owns which actor id across the cluster. Empty when ownership is
		// disabled, which is itself useful information for an agent.
		var payload interface{}
		leases, node, enabled := a.reg.Ownership()
		if !enabled {
			payload = map[string]interface{}{"enabled": false, "node": "", "leases": []Lease{}}
		} else {
			list, err := leases.List(context.Background())
			if err != nil {
				a.sendError(reply, id, jsonRPCInternalError, "list ownership leases: "+err.Error(), nil)
				return
			}
			payload = map[string]interface{}{"enabled": true, "node": node, "leases": list}
		}
		a.sendResult(reply, id, map[string]interface{}{
			"contents": []map[string]interface{}{
				{
					"uri":      args.URI,
					"mimeType": "application/json",
					"text":     mustJSON(payload),
				},
			},
		})
	default:
		a.sendError(reply, id, jsonRPCMethodNotFound, "resource not found", nil)
	}
}

func (a *MCPServerActor) sendResult(reply string, id json.RawMessage, result interface{}) {
	resp := jsonRPCResponse{JSONRPC: "2.0", ID: id, Result: result}
	b, _ := json.Marshal(resp)
	_ = a.reg.Publish(reply, b)
}

func (a *MCPServerActor) sendError(reply string, id json.RawMessage, code int, message string, data interface{}) {
	resp := jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &jsonRPCError{Code: code, Message: message, Data: data},
	}
	b, _ := json.Marshal(resp)
	_ = a.reg.Publish(reply, b)
}

// mustJSON marshals v to JSON, panicking on error (used for IDs we control).
func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// ensure MCPServerActor implements Actor at compile time.
var _ Actor = (*MCPServerActor)(nil)

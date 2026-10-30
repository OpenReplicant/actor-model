package actors

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	kernel "actor-kernel/kernel"
)

// TestMCPPlacementPlan verifies an agent can ask where an actor belongs and
// gets a concrete node back, using live leases as the cluster view.
func TestMCPPlacementPlan(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	mcp := NewMCPServerActor("mcp", []string{"mcp.mcp.>"}, reg)
	if err := reg.Start(mcp); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	leases, err := kernel.NewLeaseStore(reg.Conn(), "mcp-placement")
	if err != nil {
		t.Fatalf("lease store: %v", err)
	}
	reg.WithOwnership(leases, "node-a")

	// A running actor puts node-a into the view.
	if err := reg.Start(&recordingActor{id: "existing", topics: []string{"x.>"}}); err != nil {
		t.Fatalf("start existing: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"name":      "placement.plan",
		"arguments": map[string]string{"actor": "candidate"},
	})
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`"p1"`),
		Method:  "tools/call",
		Params:  body,
	}
	b, _ := json.Marshal(req)
	respData, err := reg.Request("mcp.mcp.request", b, 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var resp jsonRPCResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("placement.plan returned an error: %s", resp.Error.Message)
	}
	out := string(respData)
	if !strings.Contains(out, `\"node\":\"node-a\"`) && !strings.Contains(out, `"node":"node-a"`) {
		t.Fatalf("expected node-a in the placement result, got %s", out)
	}
}

// mcpRequest sends a JSON-RPC request to the MCP server via NATS request/reply
// and returns the parsed response.
func mcpRequest(t *testing.T, reg *kernel.Registry, method string, id string) jsonRPCResponse {
	t.Helper()
	req := jsonRPCRequest{JSONRPC: "2.0", ID: json.RawMessage(`"` + id + `"`), Method: method}
	b, _ := json.Marshal(req)
	respData, err := reg.Request("mcp.mcp.request", b, 2*time.Second)
	if err != nil {
		t.Fatalf("request mcp.%s: %v", method, err)
	}
	var resp jsonRPCResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

func TestMCPServerInitialize(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	mcp := NewMCPServerActor("mcp", []string{"mcp.mcp.>"}, reg)
	if err := reg.Start(mcp); err != nil {
		t.Fatalf("start: %v", err)
	}

	resp := mcpRequest(t, reg, "initialize", "init-1")
	if resp.Error != nil {
		t.Fatalf("initialize error: %+v", resp.Error)
	}
	result, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected result object, got %T", resp.Result)
	}
	if result["protocolVersion"] != MCPProtocolVersion {
		t.Fatalf("expected protocolVersion %s, got %v", MCPProtocolVersion, result["protocolVersion"])
	}
}

func TestMCPServerToolsList(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	mcp := NewMCPServerActor("mcp", []string{"mcp.mcp.>"}, reg)
	if err := reg.Start(mcp); err != nil {
		t.Fatalf("start: %v", err)
	}

	echo := NewEchoActor("echo", []string{"echo.>"}, reg)
	if err := reg.Start(echo); err != nil {
		t.Fatalf("start echo: %v", err)
	}

	resp := mcpRequest(t, reg, "tools/list", "tools-1")
	if resp.Error != nil {
		t.Fatalf("tools/list error: %+v", resp.Error)
	}
	result, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected result object")
	}
	tools, ok := result["tools"].([]interface{})
	if !ok || len(tools) == 0 {
		t.Fatalf("expected non-empty tools list, got %v", result["tools"])
	}
}

func TestIDRegistry(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	echo := NewEchoActor("echo", []string{"echo.>"}, reg)
	if err := reg.Start(echo); err != nil {
		t.Fatalf("start echo: %v", err)
	}

	idl := reg.IDRegistry()
	if len(idl.Actors) != 1 {
		t.Fatalf("expected 1 actor in kernel.IDL, got %d", len(idl.Actors))
	}
	if idl.Actors[0].ID != "echo" {
		t.Fatalf("expected echo actor, got %s", idl.Actors[0].ID)
	}
}

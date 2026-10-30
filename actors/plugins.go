// Package actors holds the builtin actor implementations: the ones that adapt
// something outside the kernel — a process, a container, an HTTP endpoint, an
// MCP server, a browser — into an actor on the bus, plus the echo and web UI
// actors used for testing and dashboards.
//
// Nothing here is required by the kernel. The kernel only defines Actor, Msg,
// Publisher, and the Registry; these are the actors a binary chooses to make
// available. Plugins collects them in one place so a binary can say
// actors.Plugins(...) instead of maintaining its own list.
package actors

import (
	"fmt"

	kernel "actor-kernel/kernel"
)

// Options carries the process-wide settings that some builtin actors need.
// They cannot come from per-actor config: the dashboard's node identity and
// browser credentials are properties of the process, not of the dashboard.
type Options struct {
	// NodeID is this process's node identity, shown by the dashboard.
	NodeID string
	// Auth is the bus authentication config, rendered into the dashboard page so
	// the browser can connect. Empty disables credentials in the page.
	Auth kernel.AuthConfig
}

// Plugins returns every builtin actor as a kernel.Plugin, in a stable order.
// Pass the result to kernel.Instantiate; the names are the ones config files
// use in their "plugin" field.
func Plugins(opts Options) []kernel.Plugin {
	return []kernel.Plugin{
		{Name: "echo", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			return NewEchoActor(id, topics, reg), nil
		}},
		{Name: "proc", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			cmd, _ := cfg["cmd"].(string)
			return NewProcActor(id, cmd, Args(cfg), reg), nil
		}},
		{Name: "rest", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			method, _ := cfg["method"].(string)
			base, _ := cfg["base"].(string)
			return NewRestActor(id, method, base, reg), nil
		}},
		{Name: "stdio", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			cmd, _ := cfg["cmd"].(string)
			return NewStdioActor(id, cmd, Args(cfg), reg), nil
		}},
		{Name: "docker", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			cmd, _ := cfg["cmd"].(string)
			image, _ := cfg["image"].(string)
			return NewDockerActor(id, cmd, image, reg), nil
		}},
		{Name: "webui", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			addr, _ := cfg["addr"].(string)
			wsURL, _ := cfg["ws_url"].(string)
			r, ok := reg.(*kernel.Registry)
			if !ok {
				return nil, fmt.Errorf("webui: publisher is %T, need *kernel.Registry", reg)
			}
			return NewWebUIActor(id, topics, addr, wsURL, r).
				WithNodeID(opts.NodeID).
				WithBrowserAuth(opts.Auth), nil
		}},
		{Name: "webclient", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			// Authentication is the bus's responsibility (see the auth config);
			// the actor only tracks the client's declared identity and liveness.
			return NewWebClientActor(id, topics, reg), nil
		}},
		{Name: "mcp", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			r, ok := reg.(*kernel.Registry)
			if !ok {
				return nil, fmt.Errorf("mcp: publisher is %T, need *kernel.Registry", reg)
			}
			return NewMCPServerActor(id, topics, r), nil
		}},
		{Name: "mcpclient", Factory: func(id string, topics []string, cfg map[string]interface{}, reg kernel.Publisher) (kernel.Actor, error) {
			cmd, _ := cfg["cmd"].(string)
			return NewMCPClientActor(id, cmd, Args(cfg), reg), nil
		}},
	}
}

// Args extracts the "args" string slice from an actor's config.
func Args(cfg map[string]interface{}) []string {
	raw, ok := cfg["args"]
	if !ok {
		return nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, len(arr))
	for i, v := range arr {
		out[i], _ = v.(string)
	}
	return out
}

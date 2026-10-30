// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
)

// RestActor wraps an HTTP endpoint as a bus-controlled actor.
// Subscribes to rest.<id>.>:
//   - request: performs an HTTP request, publishes rest.<id>.responded with
//     the response body on success, or rest.<id>.failed on error.
//
// The request message body is a JSON object:
//
//	{"method":"GET","path":"/health","headers":{"X-Key":["v"]},"body":"..."}
type RestActor struct {
	id     string
	method string
	base   string
	reg    Publisher

	mu sync.Mutex
}

// NewRestActor creates a REST wrapper actor for the given base URL.
func NewRestActor(id, method, base string, reg Publisher) *RestActor {
	return &RestActor{
		id:     id,
		method: method,
		base:   base,
		reg:    reg,
	}
}

func (a *RestActor) ID() string       { return a.id }
func (a *RestActor) Topics() []string { return []string{"rest." + a.id + ".>"} }

func (a *RestActor) Handle(ctx context.Context, msg Msg) {
	switch msg.Subject {
	case "rest." + a.id + ".request":
		a.handleRequest(ctx, msg)
	}
}

type restRequest struct {
	Method string              `json:"method"`
	Path   string              `json:"path"`
	Header map[string][]string `json:"headers"`
	Body   string              `json:"body"`
}

func (a *RestActor) handleRequest(ctx context.Context, msg Msg) {
	var req restRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.reg.Publish("rest."+a.id+".failed", []byte("invalid request: "+err.Error()))
		return
	}

	method := req.Method
	if method == "" {
		method = a.method
	}
	url := a.base + req.Path

	r, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		a.reg.Publish("rest."+a.id+".failed", []byte("request error: "+err.Error()))
		return
	}
	if req.Header != nil {
		r.Header = req.Header
	}

	resp, err := http.DefaultClient.Do(r.WithContext(ctx))
	if err != nil {
		a.reg.Publish("rest."+a.id+".failed", []byte("request error: "+err.Error()))
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.reg.Publish("rest."+a.id+".failed", []byte("read error: "+err.Error()))
		return
	}

	a.reg.Publish("rest."+a.id+".responded", body)
}

// ensure RestActor implements Actor at compile time.
var _ Actor = (*RestActor)(nil)

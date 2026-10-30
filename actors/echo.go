package actors

import (
	"context"
	"log"
	"sync"

	kernel "actor-kernel/kernel"
)

// EchoActor subscribes to echo.>, logs each message, and republishes the body
// on msg.Reply if set. For testing and debugging.
type EchoActor struct {
	id      string
	topics  []string
	reg     kernel.Publisher
	mu      sync.Mutex
	msgs    []kernel.Msg
	handles int
}

// NewEchoActor creates an echo actor registered on the given topics.
func NewEchoActor(id string, topics []string, reg kernel.Publisher) *EchoActor {
	return &EchoActor{id: id, topics: topics, reg: reg}
}

func (a *EchoActor) ID() string       { return a.id }
func (a *EchoActor) Topics() []string { return a.topics }

func (a *EchoActor) Handle(ctx context.Context, msg kernel.Msg) {
	a.mu.Lock()
	a.msgs = append(a.msgs, msg)
	a.handles++
	a.mu.Unlock()
	log.Printf("echo %s: subject=%q reply=%q", a.id, msg.Subject, msg.Reply)
	if msg.Reply != "" {
		if err := a.reg.Publish(msg.Reply, msg.Data); err != nil {
			log.Printf("echo %s: publish reply failed: %v", a.id, err)
		}
	}
}

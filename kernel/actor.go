// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import "context"

// Msg is a message published on the bus. It mirrors a NATS message: a subject,
// a payload, and an optional reply subject for request/reply.
type Msg struct {
	Subject string // NATS subject the message was published on
	Data    []byte // payload; schema is actor-defined
	Reply   string // if set, sender expects a reply on this subject
}

// Actor is the kernel's plugin contract. An actor owns its state, declares the
// NATS subjects it subscribes to, and handles each published message. Actors
// own their state; no shared memory, no locks between actors.
type Actor interface {
	// ID returns the unique actor name within the kernel.
	ID() string
	// Topics returns the NATS subjects this actor subscribes to. Wildcards: `>`
	// matches any remaining tokens (e.g. `sensors.>` matches `sensors.temperature`),
	// `*` matches a single token.
	Topics() []string
	// Handle is called for each published message on one of the actor's topics.
	Handle(ctx context.Context, msg Msg)
}

// Publish is the convenience callback actors use to publish messages. Actors
// receive this at construction time and never touch the NATS connection directly.
type Publish func(subject string, data []byte) error

// Publisher is the minimal interface an actor needs to publish messages.
// Registry satisfies it; tests can provide a fake.
type Publisher interface {
	Publish(subject string, data []byte) error
}

// Runnable is an optional interface for actors that own resources needing an
// explicit lifecycle — an HTTP listener, a reconcile loop, a background
// goroutine. Registry.Start calls Start on such actors after subscribing, and
// Registry.Stop calls Stop before unsubscribing.
//
// This exists because an actor that starts its resources lazily "on first
// message" never starts at all if nobody publishes to it — which is exactly
// what happened to the dashboard: its HTTP listener waited for a message on
// webui.> and none ever came.
type Runnable interface {
	// Start begins the actor's background work. Called once per Start.
	Start() error
	// Stop halts it. Must be safe to call more than once.
	Stop() error
}

// ActorInfo describes a running actor for inspection (e.g. by the Web UI).
type ActorInfo struct {
	ID     string   `json:"id"`
	Topics []string `json:"topics"`
}

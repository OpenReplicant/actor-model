// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import "encoding/json"

// IDL is the standard interface description used across the actor system.
// Every actor can optionally describe itself via ActorSpec: the topics it
// subscribes to/publishes, the methods it exposes, and JSON Schema for
// message payloads. This is the IDL that MCP servers/clients, the Web UI,
// and external clients use to discover and interact with actors without
// knowing their Go types.
//
// Using JSON Schema keeps the IDL dependency-free and interoperable: it is
// the same schema language MCP uses for tool input/output, and it is language-
// neutral so actors written in any language (via stdio) can describe
// themselves.
type IDL struct {
	// Actors describes each actor in the system.
	Actors []ActorSpec `json:"actors"`
}

// ActorSpec describes a single actor's interface.
type ActorSpec struct {
	ID           string       `json:"id"`
	Topics       []TopicSpec  `json:"topics,omitempty"`
	Methods      []MethodSpec `json:"methods,omitempty"`
	Capabilities []string     `json:"capabilities,omitempty"`
	Description  string       `json:"description,omitempty"`
}

// TopicSpec describes a topic an actor subscribes to or publishes on.
type TopicSpec struct {
	Subject     string `json:"subject"`
	Direction   string `json:"direction"`        // "subscribe" or "publish"
	Schema      string `json:"schema,omitempty"` // JSON Schema ref or inline for the payload
	Description string `json:"description,omitempty"`
}

// MethodSpec describes a callable method an actor exposes (e.g. via MCP).
type MethodSpec struct {
	Name         string `json:"name"`
	Description  string `json:"description,omitempty"`
	InputSchema  string `json:"input_schema,omitempty"`  // JSON Schema for arguments
	OutputSchema string `json:"output_schema,omitempty"` // JSON Schema for return value
}

// SpecActor is an optional interface actors can implement to describe
// themselves in the standard IDL. Actors that don't implement it get a
// minimal spec derived from their ID() and Topics().
type SpecActor interface {
	Actor
	// Spec returns the actor's standard IDL description.
	Spec() ActorSpec
}

// IDRegistry returns the IDL for all registered actors.
func (r *Registry) IDRegistry() IDL {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := IDL{}
	for id, box := range r.actorBox {
		spec := ActorSpec{ID: id}
		if sa, ok := box.actor.(SpecActor); ok {
			spec = sa.Spec()
		} else {
			topics := make([]string, 0, len(r.actorSub[id]))
			for topic := range r.actorSub[id] {
				topics = append(topics, topic)
			}
			for _, t := range topics {
				spec.Topics = append(spec.Topics, TopicSpec{Subject: t, Direction: "subscribe"})
			}
		}
		out.Actors = append(out.Actors, spec)
	}
	return out
}

// MarshalJSON implements json.Marshaler for IDL.
func (idl IDL) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Actors []ActorSpec `json:"actors"`
	}{Actors: idl.Actors})
}

// ensure registry method compiles.
var _ = (*Registry).IDRegistry

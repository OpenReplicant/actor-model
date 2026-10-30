// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// jsBackend manages JetStream durability for a single actor topic.
//
// The NATS model is: a stream owns a set of subjects (the durable log), and a
// consumer is one reader's position within that stream. So the stream is
// derived from the *subject* — two actors subscribing to the same subject share
// one stream but each gets its own durable consumer, which is exactly what you
// want for a work queue or for replay.
//
// Messages are pulled into the actor's shared inbox channel with an ack
// callback. Acking confirms the message was handled; if the actor stops before
// acking, the consumer redelivers it (at-least-once).
//
// Plain NATS topics do not use a jsBackend — they go through the regular
// nc.Subscribe callback with a noop ack.
type jsBackend struct {
	mu       sync.Mutex
	js       nats.JetStreamContext // JetStream context
	stream   string                // stream name (derived from subject)
	consumer string                // durable consumer name (derived from actor)
	sub      *nats.Subscription    // pull subscription
	inbox    chan<- jsMsg          // actor's inbox (send-only)
	am       *ActorMetrics         // per-actor metrics
	subject  string                // NATS subject this backend handles
	quit     chan struct{}         // closed to stop the pull loop
	closed   bool
}

// newJsBackend creates a JetStream backend for one actor topic: it ensures the
// stream and durable consumer exist, but does not start reading until start().
func newJsBackend(js nats.JetStreamContext, actorID, subject string, inbox chan<- jsMsg, am *ActorMetrics, tc TopicConfig) (*jsBackend, error) {
	stream, err := ensureStream(js, subject, tc)
	if err != nil {
		return nil, err
	}

	consumer := sanitize(actorID)
	consumerConfig := &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 1000, // backpressure: pause delivery past N unacked
		FilterSubject: subject,
		AckWait:       30 * time.Second,
		MaxDeliver:    1000,
	}
	if _, err := js.AddConsumer(stream, consumerConfig); err != nil {
		return nil, fmt.Errorf("add consumer %q on stream %q: %w", consumer, stream, err)
	}

	return &jsBackend{
		js:       js,
		stream:   stream,
		consumer: consumer,
		inbox:    inbox,
		am:       am,
		subject:  subject,
		quit:     make(chan struct{}),
	}, nil
}

// ensureStream makes sure a stream covering subject exists and returns its
// name. It is idempotent: restarting an actor reuses the existing stream.
func ensureStream(js nats.JetStreamContext, subject string, tc TopicConfig) (string, error) {
	name := "JS_" + sanitize(subject)
	cfg := &nats.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Retention: retentionPolicy(tc.Retention),
		MaxAge:    time.Duration(tc.MaxAge),
		MaxBytes:  tc.MaxBytes,
		MaxMsgs:   tc.MaxMsgs,
	}

	if _, err := js.StreamInfo(name); err == nil {
		if _, err := js.UpdateStream(cfg); err != nil {
			return "", fmt.Errorf("update stream %q: %w", name, err)
		}
		return name, nil
	}

	addErr := error(nil)
	if _, err := js.AddStream(cfg); err == nil {
		return name, nil
	} else {
		addErr = err
	}

	// Another stream already claims this subject (e.g. a wildcard stream from
	// another actor). Reuse it rather than fighting over the subject space.
	existing, err := findStreamForSubject(js, subject)
	if err != nil {
		return "", err
	}
	if existing == "" {
		return "", fmt.Errorf("add stream %q for subject %q: %w", name, subject, addErr)
	}
	return existing, nil
}

// findStreamForSubject returns the name of an existing stream whose subject
// list already covers subject, or "" if none does. On a real error (not a
// missing stream) it returns the error.
func findStreamForSubject(js nats.JetStreamContext, subject string) (string, error) {
	for name := range js.StreamNames() {
		si, err := js.StreamInfo(name)
		if err != nil || si == nil {
			continue
		}
		if coversSubject(si.Config.Subjects, subject) {
			return name, nil
		}
	}
	return "", nil
}

// coversSubject reports whether subject is already covered by any subject
// pattern in patterns, using NATS wildcard semantics (`>` and `*`).
func coversSubject(patterns []string, subject string) bool {
	for _, p := range patterns {
		if p == subject {
			return true
		}
		if subjectMatchesPattern(p, subject) {
			return true
		}
	}
	return false
}

// subjectMatchesPattern reports whether subject matches the NATS pattern.
func subjectMatchesPattern(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	pt := splitSubject(pattern)
	st := splitSubject(subject)
	for i, tok := range pt {
		if tok == ">" {
			return len(st) >= i // `>` matches one or more remaining tokens
		}
		if i >= len(st) {
			return false
		}
		if tok != "*" && tok != st[i] {
			return false
		}
	}
	return len(pt) == len(st)
}

func splitSubject(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

// retentionPolicy maps the config retention to the nats policy.
func retentionPolicy(r RetentionPolicy) nats.RetentionPolicy {
	switch r {
	case InterestRetention:
		return nats.InterestPolicy
	case WorkQueueRetention:
		return nats.WorkQueuePolicy
	default:
		return nats.LimitsPolicy
	}
}

// start subscribes to the durable pull consumer and begins delivering messages
// to the actor's inbox. A pull subscription plus a blocking fetch loop gives
// backpressure: when the actor's mailbox is full, fetch waits instead of
// dropping messages.
func (b *jsBackend) start() error {
	sub, err := b.js.PullSubscribe(b.subject, b.consumer)
	if err != nil {
		return fmt.Errorf("pull subscribe %q: %w", b.subject, err)
	}
	b.mu.Lock()
	b.sub = sub
	b.mu.Unlock()

	go b.pullLoop(sub)
	return nil
}

// pullLoop repeatedly fetches messages from the durable consumer and pushes
// them into the actor's inbox. It exits when the backend is stopped.
func (b *jsBackend) pullLoop(sub *nats.Subscription) {
	for {
		select {
		case <-b.quit:
			return
		default:
		}

		msgs, err := sub.Fetch(1, nats.MaxWait(100*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return
		}
		for _, msg := range msgs {
			select {
			case b.inbox <- jsMsg{
				msg: Msg{Subject: msg.Subject, Data: msg.Data, Reply: msg.Reply},
				ack: func() {
					// A failed ack just means redelivery; nothing to do here.
					_ = msg.Ack()
				},
			}:
			case <-b.quit:
				return
			}
		}
	}
}

// stop closes the pull loop and the JetStream subscription.
func (b *jsBackend) stop() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.quit)
	sub := b.sub
	b.mu.Unlock()
	if sub != nil {
		return sub.Unsubscribe()
	}
	return nil
}

// sanitize converts a NATS subject or actor id into a valid JetStream stream
// or consumer name (alphanumeric, underscores, hyphens allowed).
func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

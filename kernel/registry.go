// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Registry owns the NATS connection and the subscription map. Actors receive a
// Publish callback at construction time; they never touch the NATS connection
// directly.
//
// Each actor runs on its own delivery goroutine with a private mailbox. The
// NATS callback enqueues messages into the mailbox; the actor's goroutine
// drains it and calls a.Handle. This prevents a slow or blocking actor from
// stalling other actors sharing the bus (per-actor delivery isolation).
//
// Topics can be plain NATS (fast, ephemeral, drop-if-full mailbox) or
// JetStream-backed (durable, replayable, at-least-once, backpressured instead
// of dropped). JetStream is a per-topic toggle configured via TopicConfig.
type Registry struct {
	nc  *nats.Conn
	pub Publish

	mu       sync.Mutex
	actorSub map[string]map[string]*nats.Subscription // actor id -> subject -> subscription
	actorBox map[string]*actorBox                     // actor id -> mailbox + control
	closed   bool

	metrics *Metrics

	actorTopicConfigs map[string][]TopicConfig // actor id -> topic configurations (from config file)

	// ownership, when set, makes Start claim a cluster-wide lease for the actor
	// id so at most one node runs it. Nil means local-only (the default).
	ownership *ownershipState

	// scheduler is the placement-consuming scheduler on this node, if any.
	scheduler SchedulerReporter
}

// ownershipState holds the lease store and this registry's node identity.
type ownershipState struct {
	leases *LeaseStore
	node   string

	mu      sync.Mutex
	renewer map[string]chan struct{} // actor id -> renew loop stop channel
}

// actorBox holds an actor's private mailbox and shutdown signals.
type actorBox struct {
	inbox chan jsMsg    // messages delivered to this actor (plain or JetStream)
	quit  chan struct{} // closed to tell the delivery goroutine to stop
	done  chan struct{} // closed when the delivery goroutine has exited

	am     *ActorMetrics         // per-actor observation metrics
	actor  Actor                 // the actor instance, for IDL introspection
	jsSubs map[string]*jsBackend // per-topic JetStream backends (nil if no JS topics)

	// pub is the registry's publish function, used by the panic path to report
	// supervision.failure.<id>. Nil when a box is built outside a registry.
	pub Publish

	// dedup suppresses redelivery of already-seen message ids. Nil when no
	// topic on this actor opted into deduplication.
	dedup *deduper
}

// jsMsg wraps a message delivered to an actor's inbox with an optional
// acknowledgment callback. For plain NATS topics ack is a noop. For JetStream
// topics ack confirms the message has been handled (at-least-once delivery);
// if ack is not called before the consumer reconnects, the message is
// redelivered.
type jsMsg struct {
	msg Msg
	ack func()
}

// noopAck is a noop acknowledgment for plain NATS topics.
func noopAck() {}

// NewRegistry creates a Registry around an existing NATS connection. The
// connection may be to an embedded in-process server
// (nats.Connect(nats.InProcessServer(srv))) or a remote URL
// (nats.Connect("nats://host:port")) — actors and topics are unchanged either
// way.
func NewRegistry(nc *nats.Conn) *Registry {
	r := &Registry{
		nc:                nc,
		actorSub:          make(map[string]map[string]*nats.Subscription),
		actorBox:          make(map[string]*actorBox),
		metrics:           NewMetrics(),
		actorTopicConfigs: make(map[string][]TopicConfig),
	}
	r.pub = func(subject string, data []byte) error {
		r.metrics.MessagesPublished.Inc()
		return r.nc.Publish(subject, data)
	}
	return r
}

// StoreTopicConfigs records the topic configurations for an actor, as parsed
// from a config file. Start() consults these to decide which topics get
// JetStream durability. Calling Start without stored configs treats all topics
// as plain NATS (the default).
func (r *Registry) StoreTopicConfigs(id string, cfgs []TopicConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actorTopicConfigs[id] = cfgs
}

// WithOwnership enables cluster-wide actor ownership. Each Start claims a lease
// for the actor id and holds it with a renew heartbeat; a second registry on
// another node is refused rather than duplicating the actor onto the bus.
//
// node should identify the machine, not the process: two registries in one
// process that should be treated as the same owner must pass the same value.
func (r *Registry) WithOwnership(leases *LeaseStore, node string) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if node == "" {
		node = "unknown"
	}
	r.ownership = &ownershipState{
		leases:  leases,
		node:    node,
		renewer: make(map[string]chan struct{}),
	}
	return r
}

// Ownership reports whether cluster-wide ownership is enabled and, if so, the
// lease store and this registry's node identity.
func (r *Registry) Ownership() (*LeaseStore, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ownership == nil {
		return nil, "", false
	}
	return r.ownership.leases, r.ownership.node, true
}

// SetScheduler records the scheduler running on this node so the dashboard and
// IDL can report scheduler state without reaching into the actor graph.
func (r *Registry) SetScheduler(s SchedulerReporter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduler = s
}

// Scheduler returns the scheduler recorded on this node, or nil.
func (r *Registry) Scheduler() SchedulerReporter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scheduler
}

// WithMetrics replaces the registry's metrics collector.
func (r *Registry) WithMetrics(m *Metrics) *Registry {
	r.mu.Lock()
	r.metrics = m
	r.mu.Unlock()
	return r
}

// Metrics returns the registry's metrics collector.
func (r *Registry) Metrics() *Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.metrics
}

// JetStream returns the JetStream context for the registry's NATS connection.
// Returns an error if the connection is closed or JetStream is not enabled on
// the server. Callers can use this to publish directly to JetStream streams
// (bypassing the plain NATS publish path) for durable topics.
func (r *Registry) JetStream() (nats.JetStreamContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.nc == nil || r.nc.IsClosed() {
		return nil, fmt.Errorf("registry: connection closed")
	}
	return r.nc.JetStream()
}

// Start subscribes actor a to each of its topics and launches a dedicated
// delivery goroutine for the actor. Messages are delivered to the actor's
// private mailbox; the goroutine drains the mailbox and calls a.Handle.
// Starting an actor on a topic it already subscribes to replaces that
// subscription.
//
// Topics configured with JetStream (via StoreTopicConfigs) use a durable
// stream + consumer for at-least-once delivery instead of the plain mailbox
// enqueue. JetStream topics block on backpressure rather than dropping.
//
// When ownership is enabled (WithOwnership), Start first claims the actor's
// cluster-wide lease. If another node holds it, Start returns an
// *OwnershipError and the actor is not subscribed — so an actor id means one
// live owner rather than two nodes both consuming the same subject.
func (r *Registry) Start(a Actor) error {
	// The lease claim is a network round trip; it must not happen under r.mu.
	if err := r.claimOwnership(a.ID()); err != nil {
		return err
	}

	// Any failure past this point must give the lease back. Leaving it held
	// would make the actor unstartable on every node, including this one, until
	// the TTL expired.
	if _, err := r.startLocked(a); err != nil {
		r.stopRenewal(a.ID())
		return err
	}

	// Start the ownership heartbeat now that the actor is actually running.
	r.startRenewal(a.ID())
	return nil
}

// startLocked performs the subscription work of Start. It returns an error
// without cleaning up; Start handles the lease.
func (r *Registry) startLocked(a Actor) (bool, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false, fmt.Errorf("registry: actor %q started after close", a.ID())
	}

	id := a.ID()
	box, exists := r.actorBox[id]
	if !exists {
		box = &actorBox{
			inbox:  make(chan jsMsg, 256),
			quit:   make(chan struct{}),
			done:   make(chan struct{}),
			am:     r.metrics.ActorMetrics(id),
			actor:  a,
			jsSubs: make(map[string]*jsBackend),
			pub:    r.pub,
		}
		r.actorBox[id] = box
	}

	subs, ok := r.actorSub[id]
	if !ok {
		subs = make(map[string]*nats.Subscription)
		r.actorSub[id] = subs
	}

	// Topic configurations from config file (plain NATS if not configured).
	tcMap := make(map[string]TopicConfig)
	for _, tc := range r.actorTopicConfigs[id] {
		tcMap[tc.Subject] = tc
		if tc.Dedup && box.dedup == nil {
			box.dedup = newDeduper(time.Duration(tc.DedupWindow))
		}
	}

	js, jsErr := r.nc.JetStream()
	for _, topic := range a.Topics() {
		if old, exists := subs[topic]; exists {
			old.Unsubscribe()
		}

		tc, hasTC := tcMap[topic]
		if hasTC && tc.JetStream && jsErr == nil {
			// JetStream-backed topic: durable stream + consumer.
			b, err := newJsBackend(js, id, topic, box.inbox, box.am, tc)
			if err != nil {
				r.mu.Unlock()
				return false, fmt.Errorf("jetstream backend %q for actor %q: %w", topic, a.ID(), err)
			}
			if err := b.start(); err != nil {
				r.mu.Unlock()
				return false, fmt.Errorf("jetstream start %q for actor %q: %w", topic, a.ID(), err)
			}
			box.jsSubs[topic] = b
			continue
		}

		// Plain NATS topic: fast, ephemeral, drop-if-full mailbox.
		b := box
		sub, err := r.nc.Subscribe(topic, func(msg *nats.Msg) {
			// Non-blocking enqueue: never stall the NATS delivery path.
			// If the actor's mailbox is full, the message is dropped.
			select {
			case b.inbox <- jsMsg{msg: Msg{Subject: msg.Subject, Data: msg.Data, Reply: msg.Reply}, ack: noopAck}:
				b.am.MailboxDepth.Inc()
			default:
				b.am.MessagesReceived.Inc() // enqueue attempt
				b.am.Dropped.Inc()
			}
		})
		if err != nil {
			r.mu.Unlock()
			return false, fmt.Errorf("subscribe %q for actor %q: %w", topic, a.ID(), err)
		}
		subs[topic] = sub
	}

	// Launch the actor's delivery goroutine once (not on resubscribe).
	if !exists {
		go deliver(a, box, r.metrics)
		r.metrics.ActorsRunning.Inc()

		// Actors that own resources (HTTP listeners, loops) start them here
		// rather than lazily on first message, which never happens if nothing
		// publishes to them.
		if run, ok := a.(Runnable); ok {
			if err := run.Start(); err != nil {
				close(box.quit)
				delete(r.actorBox, id)
				delete(r.actorSub, id)
				r.metrics.ActorsRunning.Dec()
				r.mu.Unlock()
				return false, fmt.Errorf("start actor %q: %w", a.ID(), err)
			}
		}
	}

	r.mu.Unlock()
	return true, nil
}

// claimOwnership takes the actor's lease when ownership is enabled. It returns
// an *OwnershipError when another node holds the lease or the lease cannot be
// established at all — ownership is not best-effort: running an actor without
// the lease it was configured to require is exactly the duplicate-actor bug the
// lease exists to prevent.
func (r *Registry) claimOwnership(id string) error {
	r.mu.Lock()
	own := r.ownership
	r.mu.Unlock()
	if own == nil {
		return nil
	}

	claimed, err := own.leases.Claim(context.Background(), id, own.node)
	if err != nil {
		return &OwnershipError{Actor: id, Node: own.node, Err: err}
	}
	if !claimed {
		var holder string
		if lease, ok, oerr := own.leases.Owner(context.Background(), id); oerr == nil && ok {
			holder = lease.Node
		}
		return &OwnershipError{Actor: id, Node: own.node, Owner: holder}
	}
	return nil
}

// startRenewal keeps the actor's lease alive while it runs. The renew interval
// is a third of the TTL, so a single missed renew does not lose ownership.
func (r *Registry) startRenewal(id string) {
	r.mu.Lock()
	own := r.ownership
	r.mu.Unlock()
	if own == nil {
		return
	}

	own.mu.Lock()
	if _, running := own.renewer[id]; running {
		own.mu.Unlock()
		return // already renewing (resubscribe of a running actor)
	}
	stop := make(chan struct{})
	own.renewer[id] = stop
	own.mu.Unlock()

	interval := own.leases.ttl() / 3
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				held, err := own.leases.Renew(context.Background(), id, own.node)
				if err != nil {
					slog.Error("ownership renew failed", "actor", id, "node", own.node, "err", err)
					continue
				}
				if !held {
					// The lease was taken over. Merely logging would leave two
					// nodes consuming the same subject — the exact bug the lease
					// exists to prevent — so the local actor is shut down.
					slog.Error("lost actor ownership; revoking local actor", "actor", id, "node", own.node)
					r.revokeActor(id)
					return
				}
			}
		}
	}()
}

// revokeActor tears down a running actor after it has lost its lease. It is the
// enforcement half of ownership: detection without revocation leaves two live
// owners on the bus.
func (r *Registry) revokeActor(id string) {
	r.mu.Lock()
	box, ok := r.actorBox[id]
	if ok {
		for topic, sub := range r.actorSub[id] {
			_ = sub.Unsubscribe()
			delete(r.actorSub[id], topic)
		}
		delete(r.actorSub, id)
		for topic, b := range box.jsSubs {
			_ = b.stop()
			delete(box.jsSubs, topic)
		}
		close(box.quit)
		delete(r.actorBox, id)
	}
	r.mu.Unlock()

	if ok {
		slog.Warn("actor revoked after losing ownership", "actor", id)
	}
}

// stopRenewal halts the renew loop and releases the actor's lease, so another
// node can take over immediately instead of waiting for expiry.
func (r *Registry) stopRenewal(id string) {
	r.mu.Lock()
	own := r.ownership
	r.mu.Unlock()
	if own == nil {
		return
	}

	own.mu.Lock()
	if stop, running := own.renewer[id]; running {
		close(stop)
		delete(own.renewer, id)
	}
	own.mu.Unlock()

	if err := own.leases.Release(context.Background(), id, own.node); err != nil {
		slog.Error("release ownership", "actor", id, "node", own.node, "err", err)
	}
}

// deliver runs on the actor's dedicated goroutine, draining its mailbox and
// dispatching each message to a.Handle. Each Handle call is wrapped in an
// OpenTelemetry span carrying the NATS subject and actor id as attributes.
// JetStream messages are acked after Handle returns; plain NATS messages use a
// noop ack.
//
// A panicking handler is recovered per message rather than allowed to unwind
// out of this goroutine. Without recovery the goroutine died on the first
// panic, leaving the actor subscribed but permanently deaf — and invisible to
// the supervisor, which only learns about failures through
// supervision.failure.<id>. The panic is counted, logged, and published on
// that subject so the failure is observable and actionable.
func deliver(actor Actor, box *actorBox, metrics *Metrics) {
	defer func() {
		close(box.done)
		metrics.ActorsRunning.Dec()
	}()
	for {
		select {
		case jm, ok := <-box.inbox:
			if !ok {
				return
			}
			metrics.MessagesReceived.Inc()
			box.am.MailboxDepth.Dec()

			// Inbox deduplication: drop a message whose id we have already
			// handled inside the window, so an at-least-once redelivery does
			// not run a non-idempotent handler twice.
			if box.dedup != nil {
				if id := messageID(jm.msg.Data); id != "" && box.dedup.Seen(id) {
					box.am.Duplicates.Inc()
					jm.ack() // ack the duplicate; it is already handled
					continue
				}
			}

			ctx := withActorSpan(context.Background(), actor, jm.msg.Subject, func(ctx context.Context) {
				start := time.Now()
				handled := runHandle(actor, jm.msg, box, ctx)
				elapsed := time.Since(start).Seconds()
				if !handled {
					return // panicked; the panic path recorded its own outcome
				}
				box.am.Handles.Inc()
				box.am.HandleDuration.Set(elapsed)
				metrics.HandleDuration.Set(elapsed)
				metrics.HandleDurationHist.Observe(elapsed)
			})
			_ = ctx
			// Ack even after a panic: leaving the message unacked would make the
			// consumer redeliver a poison message forever.
			jm.ack() // ack after handle; JetStream redelivers if not acked
		case <-box.quit:
			return
		}
	}
}

// runHandle calls actor.Handle, recovering any panic. It reports whether the
// handler returned normally.
func runHandle(actor Actor, msg Msg, box *actorBox, ctx context.Context) (handled bool) {
	defer func() {
		if r := recover(); r != nil {
			handled = false
			box.am.Panics.Inc()
			// A nil publisher means the box was created without a registry
			// (tests constructing one directly); logging still applies.
			slog.Error("actor handler panicked",
				"actor", actor.ID(),
				"subject", msg.Subject,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			if box.pub != nil {
				payload, _ := json.Marshal(map[string]interface{}{
					"actor":   actor.ID(),
					"subject": msg.Subject,
					"panic":   fmt.Sprint(r),
					"stack":   string(debug.Stack()),
					"time":    time.Now().UTC().Format(time.RFC3339),
				})
				_ = box.pub("supervision.failure."+actor.ID(), payload)
			}
		}
	}()
	actor.Handle(ctx, msg)
	return true
}

// ListActors returns information about all currently registered actors.
func (r *Registry) ListActors() []ActorInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ActorInfo, 0, len(r.actorBox))
	for id := range r.actorBox {
		topics := make([]string, 0, len(r.actorSub[id]))
		for topic := range r.actorSub[id] {
			topics = append(topics, topic)
		}
		// JetStream topics have no plain NATS subscription, so they would be
		// invisible here. Merge them in from the box's backends.
		if box := r.actorBox[id]; box != nil {
			for topic := range box.jsSubs {
				topics = append(topics, topic)
			}
		}
		sort.Strings(topics)
		out = append(out, ActorInfo{ID: id, Topics: topics})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Stop unsubscribes all subscriptions for actor a and shuts down its delivery
// goroutine. It is safe to call on an actor that was never started. Other
// actors sharing the same topics are not affected.
func (r *Registry) Stop(a Actor) error {
	r.mu.Lock()
	id := a.ID()
	box, ok := r.actorBox[id]
	if !ok {
		r.mu.Unlock()
		return nil
	}

	subs, hasSubs := r.actorSub[id]
	if hasSubs {
		for topic, sub := range subs {
			if err := sub.Unsubscribe(); err != nil {
				r.mu.Unlock()
				return err
			}
			delete(subs, topic)
		}
		delete(r.actorSub, id)
	}

	// Close JetStream backends for this actor.
	if box.jsSubs != nil {
		for topic, b := range box.jsSubs {
			if err := b.stop(); err != nil {
				r.mu.Unlock()
				return err
			}
			delete(box.jsSubs, topic)
		}
	}

	// Signal the delivery goroutine to stop. It exits after its current
	// Handle returns (or immediately if idle). We do not wait for it here,
	// to avoid holding the registry lock while a handler may block.
	close(box.quit)
	delete(r.actorBox, id)
	r.mu.Unlock()

	// Stop actor-owned resources (HTTP listener, loops) before releasing the
	// lease, so a takeover cannot race a half-stopped actor.
	if run, ok := a.(Runnable); ok {
		if err := run.Stop(); err != nil {
			slog.Error("stop actor resources", "actor", id, "err", err)
		}
	}

	// Drop the ownership lease so another node can take the actor over now
	// rather than waiting for the lease to expire.
	r.stopRenewal(id)
	return nil
}

// Publish publishes data on subject through the registry's NATS connection.
func (r *Registry) Publish(subject string, data []byte) error {
	r.mu.Lock()
	nc := r.nc
	closed := r.closed
	r.mu.Unlock()
	if closed || nc == nil || nc.IsClosed() {
		return fmt.Errorf("registry: connection closed")
	}
	return nc.Publish(subject, data)
}

// Request publishes data on subject and waits up to timeout for a reply.
// If no timeout is given, nats.go's default request timeout applies.
//
// The registry lock is released while waiting for the reply so that a handler
// running on this same registry can call Publish to send the reply back —
// otherwise Publish would block on the lock held by Request.
func (r *Registry) Request(subject string, data []byte, timeout ...time.Duration) ([]byte, error) {
	r.mu.Lock()
	nc := r.nc
	closed := r.closed
	r.mu.Unlock()
	if closed || nc == nil || nc.IsClosed() {
		return nil, fmt.Errorf("registry: connection closed")
	}
	var t time.Duration
	if len(timeout) > 0 {
		t = timeout[0]
	}
	msg, err := nc.Request(subject, data, t)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

// Close closes all subscriptions and the NATS connection. It is idempotent.
func (r *Registry) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	for _, subs := range r.actorSub {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}
	r.actorSub = make(map[string]map[string]*nats.Subscription)
	// Track which actors held leases so they can be released after the lock is
	// dropped (release is a network round trip).
	var owned []string
	if r.ownership != nil {
		for id := range r.actorBox {
			owned = append(owned, id)
		}
	}
	for _, box := range r.actorBox {
		close(box.quit)
	}
	r.actorBox = make(map[string]*actorBox)
	r.mu.Unlock()

	for _, id := range owned {
		r.stopRenewal(id)
	}

	r.mu.Lock()
	if r.nc != nil {
		r.nc.Close()
	}
	r.mu.Unlock()
	return nil
}

// Pub returns the bound publish callback for actors. Actors use this to publish
// messages without touching the NATS connection directly.
func (r *Registry) Pub() Publish {
	return r.pub
}

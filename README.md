# Glaze — Actor Kernel Framework

A Go framework for building software as a network of actors on a NATS message bus.

## What this is

- **Layer 1** — `actor-kernel` (this doc): an in-process message bus with a registry of actors. Each actor owns mutable state, subscribes to NATS topics, handles messages on its own goroutine with a private mailbox. No shared memory between actors.
- **Layer 2** — `world-model`: epistemic knowledge system — world models, sensors, interpretation, belief, truth maintenance (separate module).
- **Layer 3** — `world-model agent`: MAPE-K loop (Monitor, Analyze, Plan, Execute, Knowledge) that builds models of systems it can build, control, and optimize (separate module).

Builtin actors: `echo` (debug), `proc` (OS process wrapper), `rest` (HTTP wrapper), `stdio` (persistent interpreter), `docker` (container wrapper), `supervisor` (restart with backoff), `webui` (admin dashboard), `webclient` (browser worker node), `mcp` (MCP server), `mcpclient` (MCP client wrapper).

**Standard IDL.** Every actor can describe itself via the standard IDL (`kernel/idl.go`): topics, methods, JSON Schema payloads. Actors implement the optional `SpecActor` interface; the registry exposes `IDRegistry()`. JSON Schema keeps it dependency-free and language-neutral — the same schema language MCP uses.

**MCP.** The `mcp` actor exposes the kernel as an MCP server over NATS (JSON-RPC 2.0): `initialize`, `tools/list`, `tools/call`, `resources/list`, `resources/read`. The `mcpclient` actor wraps an external MCP server as a bus actor (stdio transport), mirroring how `rest` wraps an HTTP endpoint.

The kernel and world-model are separate Go modules. The world-model imports only `actor-kernel/kernel` interfaces — no coupling to NATS internals.

## What makes it valuable

**NATS as the substrate, not an afterthought.** Every actor speaks the same subject-based protocol. Clustering is a URL swap (`-nats_url`) — actors and topics are unchanged. Wrapper actors (`proc`, `docker`, `rest`) can move to their own nodes without code changes.

**Actor identity, not just actor plumbing.** A distributed bus alone gives you pub/sub: start `job-1` on two nodes and both consume every message. With ownership enabled, the actor id is a JetStream KV lease — claiming it is a compare-and-set, so one node wins, the winner renews, and a loser is refused rather than silently duplicating the actor. Lose the lease and the local actor is revoked. This is what makes an actor id mean one thing cluster-wide.

**One binary, two faces.** The embedded NATS server accepts in-process actors (`nats.InProcessServer(srv)`) and remote network clients (`srv.ClientURL()`) on the same instance, without `DontListen`. It's simultaneously an in-process actor host and a cluster peer.

**Observability built in, not bolted on.** Per-actor metrics (messages received, handles, duration, dropped, mailbox depth), kernel-level counters + handle-duration histogram, structured `slog` logging, OTel spans on every `Handle`, and NATS server logs to a file for OTel `filelog` → OpenObserve/Last9. Zero external deps for metrics — dependency-free Prometheus text format.

**Durability is a toggle, not an architecture.** Any topic can be promoted to JetStream by writing it as an object instead of a string. The same actor can handle ephemeral telemetry on a plain subject and unlosable commands on a durable one; the kernel derives the stream from the subject and the consumer from the actor id. Fast path stays fast, and you only pay for the log where losing a message is a correctness bug.

**The cluster converges on an assignment.** Ownership leases give exactly one owner per actor id; placement gives a deterministic answer for where an actor belongs; the scheduler acts on that answer, idempotently, stopping an actor that moves; supervision caps restart intensity so a crash loop cannot masquerade as progress; dedup makes at-least-once redelivery safe for non-idempotent handlers.

**A dashboard that shows the machinery.** The `webui` actor serves a live view of actors, leases, scheduler decisions and traffic, with controls that are ordinary bus publishes rather than a privileged API. It is the sample: start the kernel, open a browser, watch it work.

**Auth is NATS's, not a kernel invention.** One config block selects `none`, `userpass`, `token`, `users`, `nkey`, `creds`, or mutual `tls`, applied to the embedded server and the kernel's own connection together. Roles are NATS subject permissions — wildcards, allow/deny — so the kernel inherits NATS's expressiveness instead of defining a weaker parallel model.

**Browser as a first-class node.** The `webui` actor serves an Alpine.js dashboard whose browser client connects to NATS over WebSocket and publishes/subscribes directly — the browser is an actor-network participant, not a privileged Go-side gatekeeper. The `webclient` actor goes further: the browser is a worker node that receives tasks and publishes results.

**Polyglot actors.** `stdio` wraps any interpreter as a persistent STDIO process speaking a JSON `Msg` protocol — actors can be written in any language without compiling Go.

## What it is NOT

- Not a governed change-set ledger, not a database, not Morphogen.
- Actor state is in-memory; state durability (JetStream KV, snapshots) stays the actor's responsibility. Message durability is different and is a per-topic toggle.
- Not a workflow engine. JetStream gives you a durable log and at-least-once delivery; it does not give you exactly-once, sagas, or transactional coordination across actors.
- Not a virtual-actor runtime. Ownership gives one owner per actor id, placement gives a destination, and the scheduler starts and stops on that basis — but there is no migration of running state and no cluster membership protocol. A moved actor restarts fresh on its new node. See [Actor ownership](#actor-ownership-one-owner-per-actor-id) and [Scheduler](#scheduler-turning-placement-into-starts).
- Not ready for layers 1–3 end-to-end wiring yet — that's deferred.

## Layout

```
actor-kernel/
├── go.mod                      # module actor-kernel, Go 1.26
├── kernel/                     # the kernel — no builtin actors here
│   ├── actor.go                # Msg, Actor, Publish, Publisher
│   ├── registry.go             # Registry: bus connection + subscription map
│   ├── idl.go                  # standard IDL (ActorSpec, TopicSpec, MethodSpec)
│   ├── tracing.go              # OpenTelemetry tracing hooks
│   ├── metrics.go              # Prometheus text-format metrics
│   ├── jetstream.go            # per-topic JetStream backend (stream + consumer)
│   ├── lease.go                # cluster-wide actor ownership leases
│   ├── placement.go            # where an actor should run (pure policy)
│   ├── scheduler.go            # acts on placement: start/stop/idempotent reconcile
│   ├── dedup.go                # inbox deduplication for at-least-once topics
│   ├── natsauth.go             # NATS auth strategies (server + client)
│   ├── kv.go                   # JetStream KV store
│   ├── supervisor.go           # SupervisorActor builtin (supervision.> — restart on failure)
│   └── *_test.go               # tests: registry, delivery, JetStream, leases, scheduler
├── actors/                     # builtin actors: everything that adapts the outside world
│   ├── plugins.go              # Plugins(): the whole builtin set as one []kernel.Plugin
│   ├── echo.go                 # EchoActor builtin (echo.>)
│   ├── proc.go                 # ProcActor builtin (proc.<id>.> — OS process wrapper)
│   ├── rest.go                 # RestActor builtin (rest.<id>.> — HTTP wrapper)
│   ├── stdio.go                # StdioActor builtin (stdio.<id>.> — persistent interpreter)
│   ├── docker.go               # DockerActor builtin (docker.<id>.> — container wrapper)
│   ├── webui.go                # WebUIActor builtin (webui.> — admin dashboard)
│   ├── index.html              # the dashboard page (embedded by webui.go)
│   ├── webclient.go            # WebClientActor builtin (webclient.<id>.> — browser worker)
│   ├── mcp.go                  # MCPServerActor builtin (mcp.<id>.> — MCP over NATS)
│   ├── mcp_client.go           # MCPClientActor builtin (mcpclient.<id>.> — MCP wrapper)
│   └── *_test.go               # tests: one per adapter, plus the dashboard
└── cmd/actor-kernel/
    └── main.go                 # binary: reads JSON config, starts NATS bus
```

The dependency only ever points one way: `actors` imports `kernel`, never the
reverse. `kernel` defines the contracts; `actors` is a library of adapters a
binary opts into. That is what makes a minimal kernel possible — a binary that
wants only the registry and its own actors never compiles the dashboard, Docker
support, or the MCP client.


## Core contract

```go
type Msg struct {
    Subject string
    Data    []byte
    Reply   string
}

type Actor interface {
    ID() string
    Topics() []string
    Handle(ctx context.Context, msg Msg)
}

type Publisher interface {
    Publish(subject string, data []byte) error
}
```

Actors receive a `Publish` callback at construction; they never touch the NATS
connection directly. NATS topics support wildcards: `>` matches remaining
tokens (`sensors.>` matches `sensors.temperature`), `*` matches one token.

## Per-actor delivery isolation

Each actor gets its own mailbox (`chan Msg`) plus a dedicated delivery
goroutine that drains the mailbox and calls `Handle`. The NATS callback
non-blockingly enqueues into the mailbox (dropping if full), so a slow or
blocking actor stalls only itself — never other actors sharing the bus.

## Panic isolation

A handler panic is recovered per message, not allowed to unwind out of the
delivery goroutine. The actor keeps running, the panic is counted
(`actor_kernel_actor_panics_total`), logged with its stack, and published on
`supervision.failure.<id>` so the supervisor can react to it.

Both halves matter. Without recovery the goroutine dies on the first panic and
the actor stays subscribed but permanently deaf — silent, and invisible to
supervision, which only ever learns about failures from that subject. Without
the failure report, recovery would keep a broken actor alive and quietly
wrong.

A panicked JetStream message is still acked. Leaving it unacked would make the
consumer redeliver a poison message forever; recovery turns an infinite
redelivery loop into one logged failure. Handlers that must not lose a message
on panic should publish it to a dead-letter subject themselves.

## Builtin actors

| Plugin | Controls | Subscribes to | Publishes |
|--------|----------|---------------|-----------|
| `echo` | debugging | `echo.>` | `msg.Reply` with body |
| `proc` | OS process | `proc.<id>.>` | `.started`, `.stopped`, `.start-failed` |
| `rest` | HTTP endpoint | `rest.<id>.>` | `.responded` (body) / `.failed` |
| `stdio` | interpreter | `stdio.<id>.>` | `.started`, `.out` (stdout lines), `.stopped`, `.failed` |
| `docker` | Docker container | `docker.<id>.>` | `.started`, `.stopped`, `.start-failed` |
| `supervisor` | actor lifecycle | `supervision.>` | `.restarted.<id>`, `.restart-failed.<id>` |
| `webui` | admin dashboard | `webui.>` | list actors, metrics via request/reply |
| `webclient` | browser worker node | `webclient.<id>.>` | `webclient.<id>.status`, `.incoming` |
| `mcp` | MCP server | `mcp.<id>.>` | JSON-RPC responses on msg.Reply |
| `mcpclient` | MCP client wrapper | `mcpclient.<id>.>` | forwards to external MCP server |

`proc`, `stdio`, and `docker` wrap one external workload each — instance per workload.

These live in the `actors` package, not in the kernel. The kernel defines only
`Actor`, `Msg`, `Publisher`, and `Registry`; `actors.Plugins()` returns the whole
builtin set as a `[]kernel.Plugin`, and a binary decides how much of it to load:

```go
import "actor-kernel/actors"

plugins := actors.Plugins(actors.Options{NodeID: nodeID, Auth: auth})
instances, err := kernel.Instantiate(cfg, plugins, reg)
```

A binary that wants its own actor set never imports `actors`: build the list
yourself and pass it to `Instantiate`. `actors.Options` carries the two settings
an actor cannot read from its own config — the node identity shown by the
dashboard, and the bus credentials injected into the dashboard page.

## Per-actor metrics

The registry tracks per-actor metrics (created on demand via `Metrics.ActorMetrics(id)`), rendered with an `actor="<id>"` label:

- `actor_kernel_actor_messages_received_total` — enqueue attempts
- `actor_kernel_actor_handles_total` — Handle calls dispatched
- `actor_kernel_actor_handle_duration_seconds` — last handle duration
- `actor_kernel_actor_messages_dropped_total` — messages dropped (mailbox full)
- `actor_kernel_actor_mailbox_depth` — current inbox depth
- `actor_kernel_actor_panics_total` — handler panics recovered (see [Panic isolation](#panic-isolation))
- `actor_kernel_actor_duplicates_total` — messages dropped as duplicate deliveries

Plus kernel-level `actor_kernel_messages_published_total`, `actor_kernel_messages_received_total`, `actor_kernel_actors_running`, `actor_kernel_last_handle_duration_seconds` (the most recent handle), and the `actor_kernel_handle_duration_seconds` histogram.

Per-actor counters hold their own storage. They are not looked up by metric name: doing so once meant every actor shared one value, so series looked per-actor while all actors reported the same number.

All metrics are available in Prometheus text exposition format via `/metrics`.

## Structured logging

The binary logs with `log/slog`. `-log_format json` emits machine-parseable JSON (default is plain text). The embedded NATS server can also write its own log to a file with `-nats_log_file <path>` (`Logtime: true`), so an OTel Collector `filelog` receiver can tail `nats-server.log` and forward it to OpenObserve/Last9/etc. — that's the recommended way to get NATS server logs into OTel, rather than relying on an in-kernel OTel emitter.

## Web UI actor (the sample)

The `webui` plugin is the working sample: an admin dashboard that shows what
the kernel is doing and lets you drive it.

```bash
actor-kernel -config examples/cluster.json \
             -js_store_dir /tmp/js-state \
             -node_id node-a \
             -webui_addr 127.0.0.1:8099
# → http://127.0.0.1:8099
```

**What it shows.** Running actors with per-actor handles, messages, drops and
panics; ownership leases (which node owns which actor id); the scheduler's
decisions and whether each actor is running here or elsewhere; a live tail of
any subject; and the raw Prometheus text.

**What it controls.** Start an actor (id, plugin, topics), stop one, publish to
any subject, and watch any subject. Controls are ordinary NATS publishes —
`supervision.start.<id>` / `supervision.stop.<id>` — consumed by the scheduler.
The dashboard is a peer on the bus, not a privileged side channel.

It serves `webui.>` reads, which the browser cannot derive on its own:

- `webui.list.actors` → running actors and their subjects
- `webui.metrics` → Prometheus text
- `webui.owners` → ownership leases, or `{"enabled": false}`
- `webui.scheduler` → this node's scheduler decisions

The page is rendered server-side with the WebSocket URL, node id, and — when
the bus requires auth — credentials injected (`{{WS_URL}}`, `{{NODE_ID}}`,
`{{AUTH}}`). That last one deserves care: anything the page can read, anyone
who can load the page can read. It is appropriate for an admin dashboard on a
private network and not appropriate on the open internet; terminate TLS and
authenticate at a proxy in front of it.

The listener opens when the actor starts, not on first message. That distinction
mattered: an earlier version started it lazily, so a dashboard nobody published
to never came up at all.

With auth enabled, the WebSocket endpoint needs TLS or an explicit `NoTLS`:
NATS refuses a WebSocket listener without TLS unless told otherwise. The binary
sets `NoTLS` for local use and accepts `-webui_tls_cert`/`-webui_tls_key` for a
real `wss://` endpoint.

## Web Client Actor

The `webclient` plugin represents a browser-based worker node on the NATS bus.
The browser connects over WebSocket and acts as a first-class actor — it
subscribes to topics, handles messages, and publishes results back:

- `webclient.<id>.register` → browser registers with a declared identity
- `webclient.<id>.heartbeat` → browser liveness check
- `webclient.<id>.disconnect` → browser signals it is going away
- `webclient.<id>.task` → task for the browser, forwarded to `.incoming`
- `webclient.<id>.result` → result published by the browser

Publishes `webclient.<id>.status` and `webclient.<id>.incoming`. Tasks are
dropped when the browser isn't connected.

**Authentication is the bus's job, not this actor's.** The browser must already
hold working NATS credentials to reach these subjects at all, and subject
permissions decide what it may use. The actor records the identity the client
declares and tracks liveness — that is all. An earlier version re-validated a
password here and exposed an `Authorize` method that was never called; it
implied protection the system did not have.

## MCP Server Actor

The `mcp` plugin exposes the actor kernel as an MCP (Model Context Protocol)
server over NATS, so LLM agents and MCP clients can discover and control
actors via JSON-RPC 2.0:

- `initialize` → server capabilities + protocol version
- `tools/list` → actors + kernel actions as MCP tools
- `tools/call` → invoke a tool
- `resources/list` → actors, IDL, metrics as MCP resources
- `resources/read` → read a specific resource (actor list, IDL, metrics)
- `ping` → heartbeat

Tools include `actors.list`, `idl.get`, and `placement.plan` (where an actor
should run, given live leases). Resources: `actor-kernel://actors`,
`actor-kernel://idl`, `actor-kernel://metrics`, `actor-kernel://owners`.

## MCP Client Actor

The `mcpclient` plugin wraps an external MCP server as an actor on the bus
(same pattern as `rest` wraps an HTTP endpoint). It starts the MCP server as
a subprocess (stdio transport) and speaks JSON-RPC 2.0 + MCP:

- `mcpclient.<id>.start` → start + initialize
- `mcpclient.<id>.stop` → stop
- `mcpclient.<id>.call` → forward a `tools/call`
- `mcpclient.<id>.list` → ask for `tools/list`

## Standard IDL

Every actor can describe itself via the standard IDL (`kernel/idl.go`): the
topics it subscribes to/publishes, the methods it exposes, and JSON Schema
for message payloads. Actors implement the optional `SpecActor` interface to
return their `ActorSpec`; actors that don't get a minimal spec derived from
their ID and topics. The registry exposes `IDRegistry()` which returns the
IDL for all registered actors.

Using JSON Schema keeps the IDL dependency-free and language-neutral, so
actors written in any language (via stdio) can describe themselves.

## Per-topic durability (JetStream toggle)

Durability is not a mode you turn on for the whole kernel — it's a property of
each topic. A topic is either a **plain NATS subject** (in-memory, fastest,
fire-and-forget) or a **JetStream-backed subject** (durable log, replayable,
at-least-once). Both kinds run side by side in the same actor.

```json
{
  "id": "orders",
  "plugin": "echo",
  "topics": [
    "orders.telemetry.>",
    { "subject": "orders.commands.>", "jetstream": true, "retention": "limits", "max_age": "1h" }
  ]
}
```

A topic written as a bare string is plain. The object form opts that subject
into JetStream and takes the usual stream knobs:

| Field | Meaning | Default |
|-------|---------|---------|
| `subject` | NATS subject (wildcards allowed) | required |
| `jetstream` | use a JetStream stream + consumer | `false` |
| `retention` | `limits`, `interest`, or `workqueue` | `limits` |
| `max_age` | how long messages are kept (`"1h"`, `"30m"`) | unlimited |
| `max_bytes` | stream size cap | unlimited |
| `max_msgs` | message count cap | unlimited |

Runtime behaviour differs meaningfully between the two:

| | plain | JetStream |
|---|---|---|
| delivery | direct callback → mailbox | pull consumer → mailbox |
| backlog | dropped if mailbox is full | backpressured, never dropped silently |
| ack | none | explicit, acked after `Handle` returns |
| down-time | messages lost | retained, replayed on restart |
| guarantees | at-most-once | at-least-once |

### How the mapping works

The NATS model is: a **stream** owns a set of subjects (the durable log) and a
**consumer** is one reader's position inside it. The kernel follows that
directly:

- **Stream is derived from the subject.** `orders.commands.>` becomes a stream
  named `JS_orders_commands__` (dots replaced; stream names can't contain
  them). Two actors subscribing to the same subject share one stream instead of
  fighting over the subject space.
- **Consumer is derived from the actor id.** Each actor gets its own durable
  consumer, so two actors on one subject each read the full stream
  independently — and a restarting actor resumes where it left off.

Ack happens after `Handle` returns. If the actor dies mid-handle the message
stays unacked and is redelivered, which is what "at-least-once" costs you:
handlers should be idempotent. `MaxAckPending` (1000) provides backpressure —
the pull loop simply stops fetching.

### Enabling it

JetStream must be on at the server. The embedded server enables it by default;
`-js_store_dir` points persistence at a real directory (otherwise NATS uses its
default temp-backed store, which is wiped between runs):

```
actor-kernel -config config.json -js_store_dir /var/lib/actor-kernel/js
```

Point every node at a distinct store directory. Two servers sharing one store
directory will disagree about which streams exist and you'll get
`subjects overlap with an existing stream` on startup.

### When to reach for it

Plain topics are the right default — they're the fast path and most
actor-to-actor chatter is fine losing a message. Flip a topic to JetStream when
losing a message is a correctness bug: commands that must execute, work queues
(`retention: "workqueue"` distributes each message to exactly one consumer
group), or streams a new actor needs to replay from the beginning.

## Durable state (JetStream KV)

Actor state is in-memory by default; durability is the actor's responsibility.
Actors can opt into an optional `KVStore` backed by a NATS JetStream KV bucket
for snapshotting state across restarts:

```go
store, err := kernel.NewKVStore(nc, "my-actor-state")
// store.Get/Put/Delete/Close
```

The `KVStore` interface (`Get`, `Put`, `Delete`, `Close`) is in `kernel/kv.go`.
Missing keys return `(nil, nil)`; delete-of-missing is a no-op. Not enabled by
default — actors instantiate and own their own store and key schema.

## NATS authentication (any strategy, not just user/pass)

NATS offers several authentication strategies and the kernel does not privilege
any of them. One config block selects the method and the kernel applies it to
both the embedded server and its own connection — the pairing matters, because
in-process connections are authenticated like any other.

```json
{ "auth": { "method": "users", "connect_as": "kernel", "users": [
    { "username": "kernel", "password": "…" },
    { "username": "worker", "password": "…",
      "permissions": { "publish": {"allow": ["jobs.>"]}, "subscribe": {"allow": ["jobs.>"]} } }
] } }
```

| Method | Server side | Client side | Use when |
|---|---|---|---|
| `none` | — | — | local, trusted only |
| `userpass` | `Username`/`Password` | `UserInfo` | one shared account |
| `token` | `Authorization` | `Token` | shared bearer secret |
| `users` | `Users` + permissions | `UserInfo` | **role-based access** |
| `nkey` | `Nkeys` | seed signature | identity without secrets |
| `creds` | operator mode (external) | creds file | decentralized JWT auth |
| `tls` | mutual TLS + `TLSVerify` | client cert | transport identity |

`"auth": "token"` is shorthand for `{"method": "token"}`.

**Authorization is NATS's, not the kernel's.** A "role" is just a set of
allowed publish/subscribe subjects, expressed in NATS's own syntax with
wildcards. The kernel deliberately does not invent a weaker permissions model
on top. `-nats_username`/`-nats_password` remain as a shorthand for `userpass`
when config says nothing.

Two limits worth knowing:

- `creds` (operator mode) can only be used **against an external server**
  (`-nats_url`). The kernel does not manage operator signing keys, so it will
  refuse to configure an embedded server in that mode rather than pretend.
- `tls` requires mutual authentication (`TLSVerify`). Plain TLS encrypts but
  authenticates nobody, which is encryption mistaken for auth.

Verified per method in `natsauth_test.go` — each test asserts the configured
credentials work *and* that a client without them is rejected.

## Placement (where should an actor run?)

Ownership answers "may I run this actor here?"; placement answers "where should
it run?". It is a pure function of the cluster view — same inputs, same node —
so two schedulers looking at the same state agree.

```go
policy := kernel.NewLeastLoadedPlacement()
view := kernel.NewLeasePlacementView(leases, nodeLabels)
node, err := policy.Place(ctx, kernel.PlacementRequest{
    Actor:    "trainer",
    Requires: map[string]string{"gpu": "true"}, // label selector
}, view)
```

Default policy is least-loaded with label constraints, an explicit `Pin`
override, and deterministic tie-breaking by node name. It is also exposed to
agents as the MCP tool `placement.plan`.

Placement is **advice**: it returns a node, it does not start anything there.
The scheduler below is what acts on it.

## Scheduler (turning placement into starts)

`SchedulerActor` closes the loop between "where should this run" and "it is
running there":

```
desired state ──▶ placement ──▶ is it mine? ──▶ start / stop / ignore
```

It consumes `scheduler.plan.<actor>` (explicit decisions) and re-evaluates its
desired set on `-scheduler_interval` (default 5s), so an actor added to the
desired set is started without anyone publishing a plan.

Three rules keep it safe:

- **Only starts what placement names for this node.** Starting elsewhere would
  defeat the ownership lease.
- **Idempotent.** An actor already running here is left alone; a scheduler that
  restarted on every tick would wipe in-memory state continuously.
- **Stops on move.** If a later plan places an actor elsewhere, the old owner
  stops it so ownership never has two holders mid-move.

It publishes `scheduler.applied.<actor>` for decisions and
`scheduler.error.<actor>` for failures — a failed start is reported, never
swallowed, because a silently absent actor is indistinguishable from one that
was never requested.

The dashboard's start/stop buttons are the on-demand path:
`supervision.start.<id>` with `{"plugin": "...", "topics": [...]}` and
`supervision.stop.<id>`. A bare publish defaults to the `echo` plugin.

It still does not migrate state. Moving a running actor means the destination
starts it fresh; actors that must survive a move persist through JetStream KV or
replay a durable topic.

## Inbox deduplication (for at-least-once topics)

JetStream is at-least-once: a consumer that dies between handling and acking
sees the message again. That is correct for a durable log and dangerous for a
handler that is not idempotent. Opt a topic into dedup and the kernel drops
repeat deliveries inside a window:

```json
{ "subject": "orders.>", "jetstream": true, "dedup": true, "dedup_window": "5m" }
```

The id comes from the JSON envelope's `"id"` field. Messages without one pass
through — dropping unknown messages would be worse than a duplicate. The table
is bounded (100k entries, oldest quarter evicted) so a flood of distinct ids
cannot exhaust memory, and entries expire on a janitor.

This is inbox-pattern deduplication, **not exactly-once delivery** — that is
not achievable across a network. It narrows the window in which a duplicate can
hurt to the window you configure.

## Actor ownership (one owner per actor id)

Without ownership the kernel has no actor *identity* across a cluster: an actor
is a local Go value subscribed to a subject, so two nodes starting `job-1` both
subscribe and both receive every message. Ownership fixes that by making the
actor id a key in a JetStream KV bucket, where claiming it is a
compare-and-set.

```go
leases, _ := kernel.NewLeaseStore(nc, "actor_ownership")
reg.WithOwnership(leases, "node-a") // node id, not process id
```

Or from the binary:

```
actor-kernel -config cluster.json -nats_url nats://bus:4222 \
             -node_id node-a -ownership_bucket actor_ownership
```

Pass the **same bucket** on every node — that bucket is what makes ownership
cluster-wide. Nodes with different buckets each believe they own the world.

### What it does

- `Start` claims the lease before subscribing. A second node starting the same
  actor id gets `*OwnershipError` naming the current owner, and does **not**
  subscribe. That is the whole point: one id, one live owner.
- The owner renews at `TTL/3` (default TTL 15s), so a single missed renew does
  not lose ownership.
- Losing the lease **revokes** the local actor: subscriptions are removed and
  the delivery goroutine stops. Detecting a takeover without acting on it would
  leave two live owners, which is the bug the lease exists to prevent.
- `Stop` releases the lease, so another node can take over immediately instead
  of waiting for expiry.
- A crash leaves the lease to expire (default 15s), after which another node
  claims it. That gap is the cost of not having a failure detector; keep the
  TTL short for fast failover, longer to tolerate GC pauses and partitions.

### What it does not do

Ownership answers enforcement only — "may I run this actor here, now?".
Placement (above) is the separate question of *where* it should run, and
deliberately does not act: it returns advice, the caller starts the actor.

- **No migration.** State does not move between nodes. An actor that needs to
  survive a takeover must persist through JetStream KV or replay from a durable
  topic. Placement chooses a node; it never relocates a running actor.
- **No cluster membership.** There is no gossip and no node-down detection. The
  cluster view is whatever leases exist; node labels are supplied by the
  operator rather than advertised.
- **Not a split-brain cure.** A node partitioned from JetStream cannot renew and
  will revoke its own actors — correct, but it means a partition stops work
  rather than continuing it on both sides.
- **Not automatic.** Nothing in the kernel starts an actor on a node it has not
  been started on. A scheduler that consumes `placement.plan` and issues starts
  is a layer above this.

What exists is the composition: one owner per actor id (lease), a deterministic
answer for where each should live (placement), bounded restart behaviour
(supervision intensity), and safe redelivery (dedup). That is enough for a
cluster to converge on an assignment. Orleans-style automatic activation and
code migration remain out of scope.

Inspect ownership with `MCPClientActor`/`MCPServerActor` resource
`actor-kernel://owners`, or `LeaseStore.List`:

```
actor=singleton node=node-2 held=true acquired=10:44:38 expires=10:45:03
```

## Supervision

`SupervisorActor` subscribes to `supervision.>`. Restart intensity is bounded:
`MaxRestarts` (default 5) within `RestartWindow` (default 60s); when exceeded
the supervisor stops restarting and publishes `supervision.abandoned.<id>`
rather than crash-looping forever, which looks like activity but makes no
progress. On
`supervision.failure.<id>` it re-establishes the actor's bus subscription
(`reg.Start`) after an exponential backoff (50ms → 5s cap), publishes
`supervision.restarted.<id>` on success or `supervision.restart-failed.<id>`
on failure. `Watch(actor)`/`Unwatch(id)` control which actors are supervised;
unsupervised failures are ignored and a restart already in flight is deduped.
Failure *detection* is decoupled — anything publishing `supervision.failure.<id>`
drives the supervisor.

## Config

`actor-kernel -config <path>` reads:

```json
{
  "transport": "inproc",
  "nats_url": "nats://cluster:4222",
  "auth": { "method": "userpass", "username": "kernel", "password": "secret" },
  "actors": [
    { "id": "echo", "plugin": "echo", "topics": ["echo.>"] },
    { "id": "proc-nginx", "plugin": "proc", "topics": ["proc.nginx.>"], "config": { "cmd": "nginx", "args": ["-g", "daemon off;"] } },
    { "id": "site", "plugin": "rest", "topics": ["rest.site.>"], "config": { "method": "GET", "base": "http://localhost:8080" } },
    { "id": "py", "plugin": "stdio", "topics": ["stdio.py.>"], "config": { "cmd": "python3", "args": ["-u", "actor.py"] } },
    { "id": "web", "plugin": "docker", "topics": ["docker.web.>"], "config": { "cmd": "docker", "image": "nginx:latest" } },
    { "id": "browser-1", "plugin": "webclient", "topics": ["webclient.browser-1.>"] },
    { "id": "mcp-server", "plugin": "mcp", "topics": ["mcp.mcp-server.>"] },
    { "id": "orders", "plugin": "echo", "topics": [
        "orders.telemetry.>",
        { "subject": "orders.commands.>", "jetstream": true, "retention": "workqueue", "max_age": "24h" }
    ] }
  ]
}
```

- `transport`: `"inproc"` (default) — all traffic in-process.
- `nats_url`: optional remote NATS server/cluster URL. When set, connects to
  that cluster instead of embedding a server. Can also be set via `-nats_url`
  flag; config takes precedence.
- `auth`: NATS authentication strategy for both the embedded server and the
  kernel's connection. Bare method name or object — see
  [NATS authentication](#nats-authentication-any-strategy-not-just-userpass).
- `actors`: list of actors to instantiate via plugin factories. Each `topics`
  entry is either a bare subject string (plain NATS) or an object enabling
  per-topic JetStream and/or dedup — see
  [Per-topic durability](#per-topic-durability-jetstream-toggle) and
  [Inbox deduplication](#inbox-deduplication-for-at-least-once-topics).

`Instantiate` takes an explicit plugin list (`[]Plugin`) instead of consulting
the global plugin registry, so each binary's actor set is explicit. Passing a
nil plugin list falls back to the package-level `pluginRegistry` for backward
compatibility. The builtin plugins (echo, proc, rest, stdio, docker, webui,
webclient, mcp, mcpclient) are collected by `actors.Plugins()` and passed here
by `cmd/actor-kernel/main.go`.

`-config` is optional. Running `actor-kernel` with no config starts an
embedded NATS node with no local actors — useful as a cluster peer.

On `SIGINT`/`SIGTERM`: stop all actors, close the bus, exit.

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-config` | `""` | JSON actor config file; omit for a bare cluster node |
| `-nats_url` | `""` | connect to a remote server/cluster instead of embedding one |
| `-nats_username` | `""` | username required from every bus client, in-process included |
| `-nats_password` | `""` | password required from every bus client |
| `-webui_addr` | `""` | serve the WebUI dashboard here (e.g. `:8080`); also opens the WebSocket endpoint |
| `-js_store_dir` | `""` | JetStream persistence directory; omit for the temp-backed default |
| `-node_id` | `""` | node identity for cluster-wide actor ownership; empty disables ownership (and the scheduler) |
| `-ownership_bucket` | `actor_ownership` | KV bucket for ownership leases; must match across nodes |
| `-scheduler_interval` | `5s` | scheduler reconcile period; `0` disables the tick (explicit plans still work) |
| `-webui_tls_cert` / `-webui_tls_key` | `""` | TLS for the dashboard WebSocket endpoint (`wss://`) |
| `-metrics_addr` | `:9090` | Prometheus `/metrics` address; `""` disables |
| `-log_format` | `text` | `text` or `json` structured logging |
| `-nats_log_file` | `""` | write the embedded NATS server log to a file (OTel filelog-friendly) |

## NATS

Uses an embedded in-process `nats-server` that accepts both in-process actors
(`nats.InProcessServer(srv)` — bypasses TCP) and remote network clients
(`srv.ClientURL()`) on the same instance. To cluster, pass `-nats_url
nats://cluster:4222` — actors and topics are unchanged.

In-process connections skip TCP, but they do **not** bypass authentication:
with `-nats_username`/`-nats_password` set, the kernel's own connection must
pass the same credentials (`nats.UserInfo`). Verified in `auth_test.go` — the
common assumption that in-process is implicitly trusted is wrong.

Authentication and authorization are standard NATS: `Users` with per-subject
`Permissions` (pub/sub allow/deny lists) give you user- or role-based access
control inside the actor network — for example an actor allowed to publish only
to `actors.echo.>` while another may subscribe broadly but publish nowhere.
Nothing is built for this in the kernel; it's configured on the NATS server and
applies to every client, in-process included. See `access_test.go`.

## Observability

### Prometheus metrics

`actor-kernel -metrics_addr :9090` exposes `http://:9090/metrics` in Prometheus
text exposition format (`actor_kernel_messages_published_total`,
`actor_kernel_messages_received_total`, `actor_kernel_actors_running`,
`actor_kernel_handle_duration_seconds`). Disable with `-metrics_addr ""`.

### OpenTelemetry tracing

Each actor `Handle` call is wrapped in an OTel span carrying `actor.id` and
`nats.subject` attributes. Spans are no-ops until an OTel SDK is registered
globally (e.g. `otel.SetTracerProvider(...)`). Instrumentation is safe to ship
before an SDK is configured.

## Layering

Layer 1 of the three-layer stack. Layer 2 (world-model / epistemic knowledge
system) and layer 3 (world-model agent / MAPE-K) live in the `world-model`
module, which imports only `actor-kernel/kernel` interfaces.

`go test ./... -race` — all tests pass, race-clean. `go vet ./...` clean.

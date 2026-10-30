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

**One binary, two faces.** The embedded NATS server accepts in-process actors (`nats.InProcessServer(srv)`) and remote network clients (`srv.ClientURL()`) on the same instance, without `DontListen`. It's simultaneously an in-process actor host and a cluster peer.

**Observability built in, not bolted on.** Per-actor metrics (messages received, handles, duration, dropped, mailbox depth), kernel-level counters + handle-duration histogram, structured `slog` logging, OTel spans on every `Handle`, and NATS server logs to a file for OTel `filelog` → OpenObserve/Last9. Zero external deps for metrics — dependency-free Prometheus text format.

**Durability is a toggle, not an architecture.** Any topic can be promoted to JetStream by writing it as an object instead of a string. The same actor can handle ephemeral telemetry on a plain subject and unlosable commands on a durable one; the kernel derives the stream from the subject and the consumer from the actor id. Fast path stays fast, and you only pay for the log where losing a message is a correctness bug.

**Auth and access control.** `-nats_username`/`-nats_password` gate every bus client, in-process included. NATS `Users` + `Permissions` (per-user pub/sub subject allow/deny) give optional user/role-based access control within the actor network.

**Browser as a first-class node.** The `webui` actor serves an Alpine.js dashboard whose browser client connects to NATS over WebSocket and publishes/subscribes directly — the browser is an actor-network participant, not a privileged Go-side gatekeeper. The `webclient` actor goes further: the browser is a worker node that receives tasks and publishes results.

**Polyglot actors.** `stdio` wraps any interpreter as a persistent STDIO process speaking a JSON `Msg` protocol — actors can be written in any language without compiling Go.

## What it is NOT

- Not a governed change-set ledger, not a database, not Morphogen.
- Actor state is in-memory; state durability (JetStream KV, snapshots) stays the actor's responsibility. Message durability is different and is a per-topic toggle.
- Not a workflow engine. JetStream gives you a durable log and at-least-once delivery; it does not give you exactly-once, sagas, or transactional coordination across actors.
- Not ready for layers 1–3 end-to-end wiring yet — that's deferred.

## Layout

```
actor-kernel/
├── go.mod                      # module actor-kernel, Go 1.26
├── kernel/
│   ├── actor.go                # Msg, Actor, Publish, Publisher
│   ├── registry.go             # Registry: bus connection + subscription map
│   ├── idl.go                  # standard IDL (ActorSpec, TopicSpec, MethodSpec)
│   ├── tracing.go              # OpenTelemetry tracing hooks
│   ├── metrics.go              # Prometheus text-format metrics
│   ├── jetstream.go            # per-topic JetStream backend (stream + consumer)
│   ├── kv.go                   # JetStream KV store
│   ├── echo.go                 # EchoActor builtin (echo.>)
│   ├── proc.go                 # ProcActor builtin (proc.<id>.> — OS process wrapper)
│   ├── rest.go                 # RestActor builtin (rest.<id>.> — HTTP wrapper)
│   ├── stdio.go                # StdioActor builtin (stdio.<id>.> — persistent interpreter)
│   ├── docker.go               # DockerActor builtin (docker.<id>.> — container wrapper)
│   ├── supervisor.go           # SupervisorActor builtin (supervision.> — restart on failure)
│   ├── webui.go                # WebUIActor builtin (webui.> — admin dashboard)
│   ├── webclient.go            # WebClientActor builtin (webclient.<id>.> — browser worker)
│   ├── mcp.go                  # MCPServerActor builtin (mcp.<id>.> — MCP over NATS)
│   ├── mcp_client.go           # MCPClientActor builtin (mcpclient.<id>.> — MCP wrapper)
│   ├── actor_test.go ...       # tests: registry, echo, proc, rest, stdio, config
│   ├── registry_test.go ...
│   ├── supervisor_test.go ...  # tests: restart-on-failure, backoff, unwatch
│   ├── webui_test.go ...
│   ├── webclient_test.go ...
│   ├── mcp_test.go ...
│   └── ...
└── cmd/actor-kernel/
    └── main.go                 # binary: reads JSON config, starts NATS bus
```

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

## Per-actor metrics

The registry tracks per-actor metrics (created on demand via `Metrics.ActorMetrics(id)`), rendered with an `actor="<id>"` label:

- `actor_kernel_actor_messages_received_total` — enqueue attempts
- `actor_kernel_actor_handles_total` — Handle calls dispatched
- `actor_kernel_actor_handle_duration_seconds` — last handle duration
- `actor_kernel_actor_messages_dropped_total` — messages dropped (mailbox full)
- `actor_kernel_actor_mailbox_depth` — current inbox depth
- `actor_kernel_actor_panics_total` — handler panics recovered (see [Panic isolation](#panic-isolation))

Plus kernel-level `actor_kernel_messages_published_total`, `actor_kernel_messages_received_total`, `actor_kernel_actors_running`, `actor_kernel_last_handle_duration_seconds` (the most recent handle), and the `actor_kernel_handle_duration_seconds` histogram.

Per-actor counters hold their own storage. They are not looked up by metric name: doing so once meant every actor shared one value, so series looked per-actor while all actors reported the same number.

All metrics are available in Prometheus text exposition format via `/metrics`.

## Structured logging

The binary logs with `log/slog`. `-log_format json` emits machine-parseable JSON (default is plain text). The embedded NATS server can also write its own log to a file with `-nats_log_file <path>` (`Logtime: true`), so an OTel Collector `filelog` receiver can tail `nats-server.log` and forward it to OpenObserve/Last9/etc. — that's the recommended way to get NATS server logs into OTel, rather than relying on an in-kernel OTel emitter.

## Web UI actor

The `webui` plugin serves an admin dashboard (Alpine.js + browser NATS client over the embedded server's WebSocket endpoint) at its configured address:

```
actor-kernel -webui_addr :8080 -nats_username admin -nats_password secret
```

It subscribes to `webui.>`:
- `webui.list.actors` → JSON list of running actors
- `webui.metrics` → Prometheus metrics text

In-process actors keep working when `-nats_username`/`-nats_password` is set (they connect via `nats.InProcessServer(srv)` on the same embedded instance); only remote network clients (the browser dashboard, plain TCP) are gated by auth.

For finer-grained access control within the actor network, NATS `Users` with `Permissions` (per-user pub/sub subject allow/deny) can restrict which actors/topics a client can reach — an optional user/role-based access pattern, not enabled by default.

## Web Client Actor

The `webclient` plugin represents a browser-based worker node on the NATS bus.
The browser connects over WebSocket and acts as a first-class actor — it
subscribes to topics, handles messages, and publishes results back:

- `webclient.<id>.register` → browser registers (basic NATS username/password)
- `webclient.<id>.heartbeat` → browser liveness check
- `webclient.<id>.disconnect` → browser signals it is going away
- `webclient.<id>.task` → task for the browser, forwarded to `.incoming`
- `webclient.<id>.result` → result published by the browser

Publishes `webclient.<id>.status` and `webclient.<id>.incoming`. Tasks are
dropped when the browser isn't connected. The authenticator is pluggable —
basic NATS auth now, OIDC token validation swappable later — so the browser
can serve as a realtime UI endpoint or a worker node for arbitrary execution.

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

Resources: `actor-kernel://actors`, `actor-kernel://idl`, `actor-kernel://metrics`.

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

## Supervision

`SupervisorActor` subscribes to `supervision.>`. On
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
- `actors`: list of actors to instantiate via plugin factories. Each `topics`
  entry is either a bare subject string (plain NATS) or an object enabling
  per-topic JetStream — see [Per-topic durability](#per-topic-durability-jetstream-toggle).

`Instantiate` takes an explicit plugin list (`[]Plugin`) instead of consulting
the global plugin registry, so each binary's actor set is explicit. Passing a
nil plugin list falls back to the package-level `pluginRegistry` for backward
compatibility. Builtin plugins (echo, proc, rest, stdio, docker) are registered
in `cmd/actor-kernel/main.go` as an explicit `[]actor.Plugin`.

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

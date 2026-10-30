# Examples

## cluster.json — the working sample

One binary, one node, a dashboard you can drive.

```bash
cd actor-kernel
go build -o /tmp/actor-kernel ./cmd/actor-kernel

/tmp/actor-kernel \
  -config examples/cluster.json \
  -js_store_dir /tmp/actor-state \
  -node_id node-a \
  -webui_addr 127.0.0.1:8099
```

Open <http://127.0.0.1:8099>. The config sets a `userpass` login
(`admin`/`admin`); the dashboard page is served with those credentials
injected, so it connects to the NATS WebSocket endpoint without prompting.
Change them before putting this anywhere reachable.

### What to try

| Action | What you should see |
|---|---|
| Publish to `jobs.order-1` with body `{"id":"m1","v":1}` | `jobs` handles it; the dashboard's handles counter increments |
| Publish the same body again | handles stays at 1, **duplicates** increments — the topic has `dedup: true` |
| Start an actor `worker-1`, plugin `echo`, topics `worker.>` | a new row appears; publish to `worker.job` and its handles counter moves |
| Watch subject `>` | live traffic, including `scheduler.applied.*` decisions |
| Stop `worker-1` | the row disappears and the lease is released |

### What is under the hood

- `webui` — the dashboard itself, an actor like any other.
- `echo` — a trivial actor on `echo.>`.
- `jobs` — an `echo` actor on a **JetStream** topic with **dedup** enabled, so
  it demonstrates durable, at-least-once delivery plus duplicate suppression.
- `scheduler` — started automatically when `-node_id` is set. It consumes
  `scheduler.plan.>` and `supervision.start/stop.>` and reports its decisions on
  `scheduler.applied.>`.

The dashboard's Controls are ordinary publishes, not a privileged API. You can
do exactly what the buttons do from any NATS client:

```bash
nats pub supervision.start.worker-2 '{"plugin":"echo","topics":["worker.>"]}'
nats pub supervision.stop.worker-2 ''
nats pub scheduler.plan.worker-3 '{"node":"node-a","plugin":"echo","topics":["worker.>"]}'
```

### Two nodes

Start a second process against the same bus. Because both share the ownership
bucket, the second is refused an actor the first already owns rather than
silently duplicating it:

```bash
# node 1 embeds the bus
/tmp/actor-kernel -config examples/cluster.json -js_store_dir /tmp/s1 \
  -node_id node-a -webui_addr 127.0.0.1:8099

# node 2 connects to it
/tmp/actor-kernel -config examples/cluster.json -nats_url nats://127.0.0.1:<port> \
  -node_id node-b -webui_addr 127.0.0.1:8100 -metrics_addr :9091
```

Node 2 will report `ownership held by node "node-a"` for the static actors it
cannot claim. Actors it *can* place (for example ones started via
`scheduler.plan` naming `node-b`) run there.

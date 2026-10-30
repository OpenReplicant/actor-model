package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"actor-kernel/actors"
	actor "actor-kernel/kernel"
)

// log is the structured logger used by the binary. It emits JSON when
// -log_format json is set, plain text otherwise — either way it is machine-
// parsable and can be tailed by an OTel filelog receiver alongside the NATS
// server log file.
var log = slog.New(slog.NewTextHandler(os.Stderr, nil))

// logFormatFlag holds the -log_format command-line flag value ("text" or
// "json"). The logger is rebuilt from it right after flag parsing.
var logFormatFlag string

// natsURLFlag holds the -nats_url command-line flag value. When set, the
// binary connects to a remote NATS server/cluster instead of embedding one.
var natsURLFlag string

// natsLogFileFlag holds the -nats_log_file command-line flag value. When set,
// the embedded NATS server writes its own log to that file, so an OTel
// filelog receiver (e.g. gosuda.org's recommended approach) can tail it.
var natsLogFileFlag string

// natsUsernameFlag holds the -nats_username command-line flag value. When set,
// the embedded NATS server requires this username/password from every client
// — including in-process ones: auth is enforced before the in-process
// transport is handed out, so the kernel's own connection must pass the same
// credentials (see the nats.UserInfo option below).
var natsUsernameFlag string

// natsPasswordFlag holds the -nats_password command-line flag value.
var natsPasswordFlag string

// webUIAddrFlag holds the -webui_addr command-line flag value. When set, the
// WebUI actor serves its admin dashboard on this address.
var webUIAddrFlag string

// jsStoreDirFlag holds the -js_store_dir command-line flag value: the on-disk
// directory the embedded NATS server uses for JetStream persistence. Empty
// means the default temp-backed store.
var jsStoreDirFlag string

// nodeIDFlag holds the -node_id command-line flag value: this node's identity
// for cluster-wide actor ownership. Empty disables ownership (every node runs
// its own copy of each configured actor, the local-only default).
var nodeIDFlag string

// ownershipBucketFlag holds the -ownership_bucket command-line flag value: the
// JetStream KV bucket holding ownership leases. Empty uses the kernel default.
//
// Pass the same bucket on every node — the bucket is what makes ownership
// cluster-wide. Nodes configured with different buckets each believe they own
// the world.
var ownershipBucketFlag string

// schedulerInterval is how often the scheduler re-evaluates its desired set.
var schedulerInterval time.Duration

// webUITLSCertFlag / webUITLSKeyFlag enable TLS on the dashboard's WebSocket
// endpoint. Without them the socket is plain ws:// (NoTLS), which is fine on a
// private network and not fine exposed.
var webUITLSCertFlag string
var webUITLSKeyFlag string

// main is the actor-kernel binary entry point. It optionally reads a JSON
// config file (-config), instantiates the registered actors, starts them on a
// NATS bus (embedded in-process by default, or a remote cluster when -nats_url
// is set), and runs until SIGINT/SIGTERM. With no -config, the binary starts
// an embedded NATS node with no local actors — useful as a cluster peer.
func main() {
	flag.StringVar(&logFormatFlag, "log_format", "text", "log format: text or json")
	flag.StringVar(&natsURLFlag, "nats_url", "", "connect to remote NATS server/cluster instead of embedding one")
	flag.StringVar(&natsLogFileFlag, "nats_log_file", "", "write embedded NATS server log to this file (OTel filelog-receiver friendly)")
	flag.StringVar(&natsUsernameFlag, "nats_username", "", "username required from all bus clients, in-process included (WebSocket dashboard)")
	flag.StringVar(&natsPasswordFlag, "nats_password", "", "password required from all bus clients, in-process included (WebSocket dashboard)")
	flag.StringVar(&webUIAddrFlag, "webui_addr", "", "address to serve the WebUI admin dashboard on (e.g. :8080); empty disables the WebUI actor")
	flag.StringVar(&jsStoreDirFlag, "js_store_dir", "", "JetStream persistence directory for the embedded NATS server (enables durable topics)")
	flag.StringVar(&nodeIDFlag, "node_id", "", "node identity for cluster-wide actor ownership; empty disables ownership")
	flag.DurationVar(&schedulerInterval, "scheduler_interval", 5*time.Second, "scheduler reconcile interval; 0 disables the tick (explicit plans still work)")
	flag.StringVar(&webUITLSCertFlag, "webui_tls_cert", "", "TLS certificate for the dashboard WebSocket endpoint (enables wss)")
	flag.StringVar(&webUITLSKeyFlag, "webui_tls_key", "", "TLS key for the dashboard WebSocket endpoint")
	flag.StringVar(&ownershipBucketFlag, "ownership_bucket", "", "JetStream KV bucket for ownership leases (must match across nodes)")
	configPath := flag.String("config", "", "path to JSON actor config file")
	metricsAddr := flag.String("metrics_addr", ":9090", "address to expose Prometheus metrics on; empty disables")
	flag.Parse()

	if logFormatFlag == "json" {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}

	var cfg actor.Config
	var err error
	if *configPath != "" {
		cfg, err = actor.LoadConfig(*configPath)
		if err != nil {
			log.Error("load config", "path", *configPath, "err", err)
			os.Exit(1)
		}
	}

	// The config's nats_url takes precedence over the -nats_url flag.
	natsURL := natsURLFlag
	if cfg.NATSURL != "" {
		natsURL = cfg.NATSURL
	}

	var srv *server.Server
	var nc *nats.Conn
	var wsURL string

	// Authentication comes from config; the username/password flags are a
	// shorthand for the userpass method and only apply when config says
	// nothing. Whatever method is chosen, the same credentials are applied to
	// the embedded server (when we run one) and to this process's connection.
	auth := cfg.Auth
	if !auth.Enabled() && natsUsernameFlag != "" {
		auth = actor.AuthConfig{
			Method:   actor.AuthUserPass,
			Username: natsUsernameFlag,
			Password: natsPasswordFlag,
		}
	}
	if err := auth.Validate(); err != nil {
		log.Error("auth config", "err", err)
		os.Exit(1)
	}
	clientAuthOpts, err := auth.ClientOptions()
	if err != nil {
		log.Error("auth client options", "err", err)
		os.Exit(1)
	}

	if natsURL != "" {
		// Connect to a remote NATS server or cluster. The same Registry,
		// actors, and topics work unchanged; clustering is just a URL swap.
		// Authentication on a remote bus is the operator's NATS config; we
		// only present credentials.
		nc, err = nats.Connect(natsURL, clientAuthOpts...)
		if err != nil {
			log.Error("connect to nats", "url", natsURL, "err", err)
			os.Exit(1)
		}
		defer nc.Close()
	} else {
		// Start embedded NATS server. No DontListen, so it accepts both
		// remote network clients (via srv.ClientURL()) and in-process actors
		// (via nats.InProcessServer(srv)) on the same instance.
		opts := &server.Options{
			Host:      "0.0.0.0",
			Port:      -1, // ephemeral; use a fixed port (e.g. 4222) for cluster peers
			NoLog:     true,
			NoSigs:    true,
			LogFile:   natsLogFileFlag,
			Logtime:   true,
			JetStream: true, // per-topic durability toggle; plain topics are unaffected
			StoreDir:  jsStoreDirFlag,
		}
		if natsLogFileFlag != "" {
			opts.NoLog = false // NATS writes to LogFile instead of stdout
		}
		// Apply whichever NATS auth strategy the config selected. This also
		// covers network clients (the browser dashboard over WebSocket), and it
		// applies to in-process connections too — the kernel's own connection
		// below presents the matching credentials.
		if err := auth.ApplyServer(opts); err != nil {
			log.Error("auth server options", "err", err)
			os.Exit(1)
		}
		// WebSocket endpoint for the browser-based dashboard. The same
		// embedded server instance serves both in-process actors and
		// remote WebSocket clients; auth above applies to WebSocket too.
		if webUIAddrFlag != "" {
			opts.Websocket.Host = "0.0.0.0"
			opts.Websocket.Port = -1 // ephemeral; actual port logged below
			// NATS refuses a WebSocket listener without TLS unless NoTLS is
			// set explicitly. The dashboard is plain HTTP on a private network;
			// use -webui_tls_cert/-webui_tls_key to terminate TLS here, or put
			// the dashboard behind a TLS-terminating proxy.
			if webUITLSCertFlag == "" {
				opts.Websocket.NoTLS = true
			} else {
				opts.Websocket.TLSConfig, err = loadTLS(webUITLSCertFlag, webUITLSKeyFlag)
				if err != nil {
					log.Error("webui tls", "err", err)
					os.Exit(1)
				}
			}
		}
		srv, err = server.NewServer(opts)
		if err != nil {
			log.Error("new nats server", "err", err)
			os.Exit(1)
		}
		go srv.Start()
		if !srv.ReadyForConnections(5 * time.Second) {
			srv.Shutdown()
			log.Error("nats server not ready")
			os.Exit(1)
		}
		defer srv.Shutdown()

		inprocOpts := append([]nats.Option{nats.InProcessServer(srv)}, clientAuthOpts...)
		nc, err = nats.Connect(nats.DefaultURL, inprocOpts...)
		if err != nil {
			log.Error("connect to embedded nats", "err", err)
			os.Exit(1)
		}
		defer nc.Close()

		log.Info("embedded nats listening", "url", srv.ClientURL(), "jetstream", true, "js_store_dir", jsStoreDirFlag)
		if webUIAddrFlag != "" {
			// WebsocketURL() reports the bind address (0.0.0.0), which a browser
			// cannot connect to. The browser reaches the endpoint on the host it
			// loaded the dashboard from, so hand it a host-relative address and
			// let the page fill in location.hostname.
			wsURL = srv.WebsocketURL()
			wsURL = strings.Replace(wsURL, "0.0.0.0", "{{HOST}}", 1)
			log.Info("websocket enabled", "ws_url", srv.WebsocketURL(), "dashboard_ws_url", wsURL)
		}
	}

	reg := actor.NewRegistry(nc)
	defer reg.Close()

	// Cluster-wide actor ownership. With a node id set, every Start claims a
	// lease keyed by actor id, so an actor id resolves to exactly one live owner
	// across the cluster instead of N nodes each consuming the same subject.
	if nodeIDFlag != "" {
		leases, err := actor.NewLeaseStore(nc, ownershipBucketFlag)
		if err != nil {
			log.Error("ownership lease store", "bucket", ownershipBucketFlag, "err", err)
			os.Exit(1)
		}
		defer leases.Close()
		reg.WithOwnership(leases, nodeIDFlag)
		log.Info("actor ownership enabled", "node_id", nodeIDFlag, "bucket", leases.Bucket())
	}

	// Inject the embedded server's WebSocket URL into any webui actor config so
	// the dashboard browser client can connect to the NATS WebSocket endpoint.
	if wsURL != "" {
		for i := range cfg.Actors {
			if cfg.Actors[i].Plugin == "webui" {
				if cfg.Actors[i].Config == nil {
					cfg.Actors[i].Config = make(map[string]interface{})
				}
				cfg.Actors[i].Config["ws_url"] = wsURL
			}
		}
	}

	// Expose Prometheus metrics on :9090/metrics (default). Can be disabled
	// with -metrics_addr "".
	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", `text/plain; version=0.0.4; charset=utf-8`)
				fmt.Fprint(w, reg.Metrics().TextFormat())
			})
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				log.Error("metrics server", "addr", *metricsAddr, "err", err)
			}
		}()
	}

	// Explicit plugin list available in this binary. The builtin set lives in
	// the actors package, which owns the adapters (process, container, HTTP, MCP,
	// browser) and the dashboard; a binary that wants extra plugins appends to
	// this slice. Passing it explicitly to Instantiate keeps this binary's actor
	// set explicit and avoids colliding with the global pluginRegistry.
	plugins := actors.Plugins(actors.Options{
		NodeID: nodeIDFlag,
		Auth:   auth,
	})

	// The scheduler starts actors the placement policy assigns to this node.
	// Its factory only needs the plugin name, since a scheduled actor has no
	// static config entry — dynamic actors get default settings and a
	// dyn.<id>.> topic namespace.
	pluginByName := make(map[string]actor.Plugin, len(plugins))
	for _, p := range plugins {
		pluginByName[p.Name] = p
	}
	schedReg := actor.NewRegistrySchedulerRegistry(reg, func(id, plugin string, topics []string) (actor.Actor, error) {
		p, ok := pluginByName[plugin]
		if !ok {
			return nil, fmt.Errorf("scheduler: unknown plugin %q", plugin)
		}
		if len(topics) == 0 {
			topics = []string{"dyn." + id + ".>"}
		}
		return p.Factory(id, topics, map[string]interface{}{}, reg)
	})

	actors, err := actor.Instantiate(cfg, plugins, reg)
	if err != nil {
		log.Error("instantiate", "err", err)
		os.Exit(1)
	}

	// With ownership enabled, run a scheduler on this node so placement is
	// actually acted on instead of merely computed.
	var sched *actor.SchedulerActor
	if nodeIDFlag != "" {
		sched = actor.NewSchedulerActor("scheduler", schedReg, actor.NewLeastLoadedPlacement(), nodeIDFlag)
		sched.ReconcileInterval = schedulerInterval
	}
	if sched != nil {
		actors = append(actors, sched)
		reg.SetScheduler(sched)
	}

	// Record each actor's per-topic config before starting, so Start() knows
	// which topics are JetStream-durable and which are plain NATS.
	for _, ac := range cfg.Actors {
		if len(ac.Topics) > 0 {
			reg.StoreTopicConfigs(ac.ID, ac.Topics)
		}
	}

	startRes := actor.StartAllWithResult(reg, actors)
	if len(startRes.Errors) > 0 {
		log.Error("start all", "err", startRes.Errors[0])
		os.Exit(1)
	}
	for _, a := range startRes.Skipped {
		// Expected in a cluster: this node is not the owner. Logged, not fatal.
		log.Info("actor owned elsewhere; skipped", "actor", a.ID(), "node_id", nodeIDFlag)
	}
	actors = startRes.Started
	if sched != nil {
		// Only run the reconcile loop if this node actually claimed the
		// scheduler actor. Ownership scopes the scheduler to one node cluster-
		// wide, so a second node must not also be issuing starts.
		schedRunning := false
		for _, a := range startRes.Started {
			if a == actor.Actor(sched) {
				schedRunning = true
			}
		}
		if schedRunning {
			sched.Start()
			log.Info("scheduler running", "node_id", nodeIDFlag, "interval", schedulerInterval)
		} else {
			log.Info("scheduler owned elsewhere; not reconciling on this node", "node_id", nodeIDFlag)
		}
	}

	log.Info("actor-kernel running", "actors", len(actors))

	// Wait for shutdown signal.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down actor-kernel")

	// Stop all actors, then close the registry.
	for _, a := range actors {
		if err := reg.Stop(a); err != nil {
			log.Error("stop actor", "id", a.ID(), "err", err)
		}
	}
	if err := reg.Close(); err != nil {
		log.Error("close registry", "err", err)
	}
	log.Info("actor-kernel stopped")
}

// loadTLS loads a certificate/key pair for the dashboard WebSocket listener.
func loadTLS(certFile, keyFile string) (*tls.Config, error) {
	if keyFile == "" {
		return nil, fmt.Errorf("webui tls: cert and key must both be set")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("webui tls: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

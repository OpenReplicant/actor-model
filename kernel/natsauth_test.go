package kernel

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startAuthBus brings up an embedded server configured by auth and connects
// the kernel's own client through AuthConfig.ClientOptions — the same pairing
// the binary uses. It fails if either side disagrees.
func startAuthBus(t *testing.T, auth AuthConfig) (*nats.Conn, *server.Server) {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	if err := auth.ApplyServer(opts); err != nil {
		t.Fatalf("apply server auth: %v", err)
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("server not ready")
	}

	clientOpts, err := auth.ClientOptions()
	if err != nil {
		srv.Shutdown()
		t.Fatalf("client options: %v", err)
	}
	clientOpts = append(clientOpts, nats.InProcessServer(srv))
	nc, err := nats.Connect(nats.DefaultURL, clientOpts...)
	if err != nil {
		srv.Shutdown()
		t.Fatalf("kernel connection with configured auth: %v", err)
	}
	return nc, srv
}

// TestAuthNoneWorks verifies the default remains unauthenticated.
func TestAuthNoneWorks(t *testing.T) {
	nc, srv := startAuthBus(t, AuthConfig{})
	defer srv.Shutdown()
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("no-auth connection failed")
	}
}

// TestAuthUserPassEnforced: the configured credentials work and a network
// client without them is rejected.
func TestAuthUserPassEnforced(t *testing.T) {
	nc, srv := startAuthBus(t, AuthConfig{
		Method: AuthUserPass, Username: "kernel", Password: "secret",
	})
	defer srv.Shutdown()
	defer nc.Close()

	url := srv.ClientURL()
	if _, err := nats.Connect(url); err == nil {
		t.Fatal("unauthenticated network client was allowed in")
	}
	good, err := nats.Connect(url, nats.UserInfo("kernel", "secret"))
	if err != nil {
		t.Fatalf("correct credentials rejected: %v", err)
	}
	good.Close()
	if _, err := nats.Connect(url, nats.UserInfo("kernel", "wrong")); err == nil {
		t.Fatal("wrong password accepted")
	}
}

// TestAuthTokenEnforced: a shared bearer token, no username.
func TestAuthTokenEnforced(t *testing.T) {
	nc, srv := startAuthBus(t, AuthConfig{Method: AuthToken, Token: "s3cret-token"})
	defer srv.Shutdown()
	defer nc.Close()

	url := srv.ClientURL()
	if _, err := nats.Connect(url); err == nil {
		t.Fatal("unauthenticated client allowed with token auth")
	}
	good, err := nats.Connect(url, nats.Token("s3cret-token"))
	if err != nil {
		t.Fatalf("correct token rejected: %v", err)
	}
	good.Close()
	if _, err := nats.Connect(url, nats.Token("wrong")); err == nil {
		t.Fatal("wrong token accepted")
	}
}

// TestAuthUsersPermissionsEnforced is the role-based access case: two users,
// one restricted to a subject namespace. Permissions are NATS's own, so the
// kernel inherits whatever expressiveness NATS has rather than inventing a
// weaker role model.
func TestAuthUsersPermissionsEnforced(t *testing.T) {
	auth := AuthConfig{
		Method:    AuthUsers,
		ConnectAs: "kernel",
		Users: []AuthUser{
			{Username: "kernel", Password: "kernel-pass"},
			{
				Username: "worker",
				Password: "worker-pass",
				Permissions: &AuthPerms{
					Publish:   &SubjectRule{Allow: []string{"jobs.>"}},
					Subscribe: &SubjectRule{Allow: []string{"jobs.>"}},
				},
			},
		},
	}
	nc, srv := startAuthBus(t, auth)
	defer srv.Shutdown()
	defer nc.Close()

	worker, err := nats.Connect(srv.ClientURL(), nats.UserInfo("worker", "worker-pass"))
	if err != nil {
		t.Fatalf("worker connect: %v", err)
	}
	defer worker.Close()

	// Allowed subject works.
	sub, err := worker.SubscribeSync("jobs.new")
	if err != nil {
		t.Fatalf("worker subscribe to allowed subject: %v", err)
	}
	defer sub.Unsubscribe()
	if err := worker.Publish("jobs.new", []byte("ok")); err != nil {
		t.Fatalf("worker publish to allowed subject: %v", err)
	}
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("worker did not receive its own allowed message: %v", err)
	}

	// Denied subject is rejected. Permission violations arrive asynchronously,
	// so wait for the error rather than checking the Publish return.
	errCh := make(chan error, 1)
	worker.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case errCh <- err:
		default:
		}
	})
	if err := worker.Publish("secrets.leak", []byte("nope")); err == nil {
		select {
		case <-errCh:
			// rejected asynchronously, as expected
		case <-time.After(2 * time.Second):
			t.Fatal("worker published to a denied subject without error")
		}
	}
}

// TestAuthNKeyEnforced: NKey identity without username/password.
func TestAuthNKeyEnforced(t *testing.T) {
	// Generate a user keypair to a seed file.
	seedFile, pubKey := generateUserNKey(t)
	nc, srv := startAuthBus(t, AuthConfig{
		Method:        AuthNKey,
		NKeySeedFile:  seedFile,
		NKeyPublicKey: pubKey,
	})
	defer srv.Shutdown()
	defer nc.Close()

	if _, err := nats.Connect(srv.ClientURL()); err == nil {
		t.Fatal("unauthenticated client allowed with nkey auth")
	}
	opt, err := nats.NkeyOptionFromSeed(seedFile)
	if err != nil {
		t.Fatalf("nkey option: %v", err)
	}
	good, err := nats.Connect(srv.ClientURL(), opt)
	if err != nil {
		t.Fatalf("nkey client rejected: %v", err)
	}
	good.Close()
}

// TestAuthConfigValidation covers the shape checks, especially the methods that
// cannot be configured on an embedded server.
func TestAuthConfigValidation(t *testing.T) {
	if err := (AuthConfig{Method: "carrier-pigeon"}).Validate(); err == nil {
		t.Fatal("unknown method accepted")
	}
	if err := (AuthConfig{Method: AuthCreds}).ApplyServer(&server.Options{}); err == nil {
		t.Fatal("creds (operator mode) should not be configurable on an embedded server")
	}
	if err := (AuthConfig{Method: AuthUserPass}).ApplyServer(&server.Options{}); err == nil {
		t.Fatal("userpass without username accepted")
	}
	if err := (AuthConfig{Method: AuthUsers}).ApplyServer(&server.Options{}); err == nil {
		t.Fatal("users without any users accepted")
	}
	if err := (AuthConfig{Method: AuthTLS}).ApplyServer(&server.Options{}); err == nil {
		t.Fatal("tls without a tls block accepted")
	}
}

// TestAuthConfigStringForm verifies the short JSON form parses.
func TestAuthConfigStringForm(t *testing.T) {
	var c AuthConfig
	if err := c.UnmarshalJSON([]byte(`"token"`)); err != nil {
		t.Fatalf("string form: %v", err)
	}
	if c.Method != AuthToken {
		t.Fatalf("method = %q, want token", c.Method)
	}
	var d AuthConfig
	if err := d.UnmarshalJSON([]byte(`{"method":"userpass","username":"u","password":"p"}`)); err != nil {
		t.Fatalf("object form: %v", err)
	}
	if d.Username != "u" || d.Password != "p" {
		t.Fatalf("object form did not populate fields: %+v", d)
	}
}

package kernel

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// TestAuthAccessControl verifies NATS per-user subject permissions restrict
// what each network client can publish/subscribe to. This is the access-control
// pattern for an actor network where some actors/topics are restricted.
func TestAuthAccessControl(t *testing.T) {
	opts := &server.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
		Users: []*server.User{
			{
				Username: "admin",
				Password: "secret",
				Permissions: &server.Permissions{
					Publish:   &server.SubjectPermission{Allow: []string{">"}},
					Subscribe: &server.SubjectPermission{Allow: []string{">"}},
				},
			},
			{
				Username: "publisher",
				Password: "pw",
				Permissions: &server.Permissions{
					Publish: &server.SubjectPermission{
						Allow: []string{"actors.echo.>"},
					},
				},
			},
			{
				Username: "reader",
				Password: "pw",
				Permissions: &server.Permissions{
					Subscribe: &server.SubjectPermission{
						Allow: []string{">"},
					},
					Publish: &server.SubjectPermission{
						Deny: []string{">"},
					},
				},
			},
		},
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	defer srv.Shutdown()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}

	var pubErrs, readerErrs sync.Mutex
	var pubErrMsgs, readerErrMsgs []string

	connectWithErrs := func(user, pass string, errs *[]string, mu *sync.Mutex) *nats.Conn {
		nc, err := nats.Connect(srv.ClientURL(),
			nats.UserInfo(user, pass),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
				mu.Lock()
				*errs = append(*errs, err.Error())
				mu.Unlock()
			}))
		if err != nil {
			t.Fatalf("%s connect: %v", user, err)
		}
		return nc
	}

	// publisher can publish to actors.echo.>
	pub := connectWithErrs("publisher", "pw", &pubErrMsgs, &pubErrs)
	defer pub.Close()

	if err := pub.Publish("actors.echo.test", []byte("x")); err != nil {
		t.Fatalf("publisher publish to allowed subject: %v", err)
	}

	// publisher cannot publish to admin.> (permission denied arrives async)
	if err := pub.Publish("admin.secret", []byte("x")); err != nil {
		t.Fatalf("publisher publish to denied subject returned sync error: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	pubErrs.Lock()
	if len(pubErrMsgs) == 0 {
		t.Fatal("expected publisher to receive a permission error for denied subject")
	}
	pubErrs.Unlock()

	// reader can subscribe to everything but cannot publish.
	reader := connectWithErrs("reader", "pw", &readerErrMsgs, &readerErrs)
	defer reader.Close()

	ch := make(chan *nats.Msg, 1)
	if _, err := reader.Subscribe("actors.echo.>", func(m *nats.Msg) { ch <- m }); err != nil {
		t.Fatalf("reader subscribe: %v", err)
	}
	if err := reader.Publish("actors.echo.test", []byte("x")); err != nil {
		t.Fatalf("reader publish returned sync error: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	readerErrs.Lock()
	if len(readerErrMsgs) == 0 {
		t.Fatal("expected reader to receive a permission error for publish")
	}
	readerErrs.Unlock()

	// admin can publish to anything.
	adm, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "secret"))
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer adm.Close()
	if err := adm.Publish("admin.secret", []byte("x")); err != nil {
		t.Fatalf("admin publish to secret subject: %v", err)
	}

	// Verify reader actually receives messages (subscribe allowed).
	go adm.Publish("actors.echo.test", []byte("y"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected reader to receive published message")
	}
}

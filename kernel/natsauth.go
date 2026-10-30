// Package kernel provides the actor kernel: an in-process message bus with a
// registry of actors. Each actor owns mutable state, subscribes to topics, and
// handles messages. The kernel knows nothing about actor internals — it only
// loads, instantiates, and subscribes them per config.
package kernel

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// NATS supports several authentication strategies, and the kernel does not
// privilege any of them. Rather than exposing only -nats_username/-nats_password
// this file passes the choice through to NATS for both the embedded server and
// every client connection. Whatever NATS can enforce, the kernel can be
// configured to use:
//
//   - none            no auth (default; local in-process only)
//   - userpass        single username/password (server.Username/Password)
//   - token           shared bearer token (server.Authorization)
//   - users           named users with per-subject permissions (server.Users)
//   - nkey            NKey public key identity (server.Nkeys)
//   - creds           operator-mode JWT + NKey seed, "creds" file
//   - tls             mutual TLS with client certificates
//
// Auth is specified once in config and applied to both sides: the embedded
// server is configured to require it, and the kernel's own connection presents
// the matching credentials. That pairing matters because in-process connections
// are authenticated like any other — a server requiring auth with a client that
// presents none fails with a closed pipe, not a helpful message.

// AuthMethod names a NATS authentication strategy.
type AuthMethod string

const (
	AuthNone     AuthMethod = "none"
	AuthUserPass AuthMethod = "userpass"
	AuthToken    AuthMethod = "token"
	AuthUsers    AuthMethod = "users"
	AuthNKey     AuthMethod = "nkey"
	AuthCreds    AuthMethod = "creds"
	AuthTLS      AuthMethod = "tls"
)

// AuthConfig selects and configures a NATS authentication strategy.
//
// Only the fields relevant to Method are used; the rest are ignored so one
// config shape covers every strategy.
type AuthConfig struct {
	// Method is the authentication strategy. Empty means "none".
	Method AuthMethod `json:"method,omitempty"`

	// Username and Password are used by "userpass" for the kernel's own
	// connection, and by the embedded server as the single allowed account.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	// Token is the shared bearer token for "token".
	Token string `json:"token,omitempty"`

	// Users is the named-user table for "users". Each entry carries subject
	// permissions, which is how role-based access control is expressed: an
	// actor-network role is just a set of allowed pub/sub subjects.
	Users []AuthUser `json:"users,omitempty"`

	// ConnectAs names which entry in Users the kernel's own connection uses.
	// Empty means the first entry.
	ConnectAs string `json:"connect_as,omitempty"`

	// NKeySeedFile is the seed file for "nkey" client auth; NKeyPublicKey is
	// the corresponding public key the embedded server allows.
	NKeySeedFile  string `json:"nkey_seed_file,omitempty"`
	NKeyPublicKey string `json:"nkey_public_key,omitempty"`

	// NKeySeedFiles maps public key -> seed file for "nkey" with multiple
	// identities; used when the embedded server must allow more than one.
	NKeySeedFiles map[string]string `json:"nkey_seed_files,omitempty"`

	// CredsFile is an operator-mode "creds" file (JWT + seed) for "creds".
	CredsFile string `json:"creds_file,omitempty"`

	// TLS, when set, is used for both server and client (mutual TLS).
	TLS *TLSConfig `json:"tls,omitempty"`
}

// AuthUser is one named user with optional subject permissions. Permissions are
// expressed in NATS's own terms, not a kernel-specific role model: a "role" is
// whatever set of subjects you allow.
type AuthUser struct {
	Username    string     `json:"username"`
	Password    string     `json:"password,omitempty"`
	NKey        string     `json:"nkey,omitempty"`
	Permissions *AuthPerms `json:"permissions,omitempty"`
}

// AuthPerms mirrors NATS subject permissions for one user.
type AuthPerms struct {
	Publish   *SubjectRule `json:"publish,omitempty"`
	Subscribe *SubjectRule `json:"subscribe,omitempty"`
}

// SubjectRule is an allow/deny list of NATS subjects (wildcards allowed).
type SubjectRule struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// TLSConfig holds the file paths for a mutual-TLS setup.
type TLSConfig struct {
	CertFile   string `json:"cert_file"`
	KeyFile    string `json:"key_file"`
	CAFile     string `json:"ca_file,omitempty"`
	ServerName string `json:"server_name,omitempty"`

	// InsecureSkipVerify disables certificate verification on the client.
	// Only for local testing; it defeats the point of TLS in production.
	InsecureSkipVerify bool `json:"insecure_skip_verify,omitempty"`
}

// Enabled reports whether the config requires any authentication at all.
func (a AuthConfig) Enabled() bool { return a.Method != "" && a.Method != AuthNone }

// UnmarshalJSON accepts either a string ("userpass") or the full object form,
// so simple configs stay short.
func (a *AuthConfig) UnmarshalJSON(data []byte) error {
	var method string
	if err := json.Unmarshal(data, &method); err == nil {
		*a = AuthConfig{Method: AuthMethod(method)}
		return nil
	}
	type alias AuthConfig
	var v alias
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*a = AuthConfig(v)
	return nil
}

// ApplyServer configures the embedded NATS server to require the configured
// authentication. It is a no-op for "none" and for remote-bus deployments,
// where the operator's NATS server owns authentication.
func (a AuthConfig) ApplyServer(opts *server.Options) error {
	switch a.Method {
	case "", AuthNone:
		return nil

	case AuthUserPass:
		if a.Username == "" {
			return fmt.Errorf("auth method %q requires username", a.Method)
		}
		opts.Username = a.Username
		opts.Password = a.Password

	case AuthToken:
		if a.Token == "" {
			return fmt.Errorf("auth method %q requires token", a.Method)
		}
		opts.Authorization = a.Token

	case AuthUsers:
		if len(a.Users) == 0 {
			return fmt.Errorf("auth method %q requires at least one user", a.Method)
		}
		users := make([]*server.User, 0, len(a.Users))
		for _, u := range a.Users {
			if u.Username == "" {
				return fmt.Errorf("auth method %q: user entry missing username", a.Method)
			}
			users = append(users, &server.User{
				Username:    u.Username,
				Password:    u.Password,
				Permissions: u.Permissions.toServer(),
			})
		}
		opts.Users = users

	case AuthNKey:
		if a.NKeyPublicKey == "" && len(a.NKeySeedFiles) == 0 {
			return fmt.Errorf("auth method %q requires nkey_public_key or nkey_seed_files", a.Method)
		}
		var nkeys []*server.NkeyUser
		if a.NKeyPublicKey != "" {
			nkeys = append(nkeys, &server.NkeyUser{Nkey: a.NKeyPublicKey})
		}
		for pub := range a.NKeySeedFiles {
			nkeys = append(nkeys, &server.NkeyUser{Nkey: pub})
		}
		opts.Nkeys = nkeys

	case AuthCreds:
		// Operator mode: the server validates JWTs signed by trusted operator
		// keys. The kernel does not manage operator keys; it only presents a
		// creds file, so embedding a server in this mode is not supported.
		return fmt.Errorf("auth method %q is for connecting to an operator-mode server; it cannot be configured on the embedded server (use -nats_url)", a.Method)

	case AuthTLS:
		if a.TLS == nil {
			return fmt.Errorf("auth method %q requires a tls block", a.Method)
		}
		cfg, err := a.TLS.serverConfig()
		if err != nil {
			return err
		}
		opts.TLSConfig = cfg
		opts.TLS = true
		// Mutual TLS: require a client certificate. Without this, TLS only
		// encrypts and anyone can still connect — encryption is not auth.
		opts.TLSVerify = true

	default:
		return fmt.Errorf("unknown auth method %q", a.Method)
	}
	return nil
}

// ClientOptions returns the nats.Options that present these credentials, for
// the kernel's own connection. It is a no-op for "none".
func (a AuthConfig) ClientOptions() ([]nats.Option, error) {
	switch a.Method {
	case "", AuthNone:
		return nil, nil

	case AuthUserPass:
		return []nats.Option{nats.UserInfo(a.Username, a.Password)}, nil

	case AuthToken:
		if a.Token == "" {
			return nil, fmt.Errorf("auth method %q requires token", a.Method)
		}
		return []nats.Option{nats.Token(a.Token)}, nil

	case AuthUsers:
		u, err := a.connectUser()
		if err != nil {
			return nil, err
		}
		return []nats.Option{nats.UserInfo(u.Username, u.Password)}, nil

	case AuthNKey:
		seed := a.NKeySeedFile
		if seed == "" {
			for _, f := range a.NKeySeedFiles {
				seed = f
				break
			}
		}
		if seed == "" {
			return nil, fmt.Errorf("auth method %q requires nkey_seed_file", a.Method)
		}
		opt, err := nats.NkeyOptionFromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("load nkey seed %q: %w", seed, err)
		}
		return []nats.Option{opt}, nil

	case AuthCreds:
		if a.CredsFile == "" {
			return nil, fmt.Errorf("auth method %q requires creds_file", a.Method)
		}
		return []nats.Option{nats.UserCredentials(a.CredsFile)}, nil

	case AuthTLS:
		if a.TLS == nil {
			return nil, fmt.Errorf("auth method %q requires a tls block", a.Method)
		}
		cfg, err := a.TLS.clientConfig()
		if err != nil {
			return nil, err
		}
		return []nats.Option{nats.Secure(cfg)}, nil

	default:
		return nil, fmt.Errorf("unknown auth method %q", a.Method)
	}
}

// connectUser resolves which entry in Users the kernel's own connection uses.
func (a AuthConfig) connectUser() (AuthUser, error) {
	if len(a.Users) == 0 {
		return AuthUser{}, fmt.Errorf("auth method %q requires at least one user", a.Method)
	}
	if a.ConnectAs == "" {
		return a.Users[0], nil
	}
	for _, u := range a.Users {
		if u.Username == a.ConnectAs {
			return u, nil
		}
	}
	return AuthUser{}, fmt.Errorf("auth users: connect_as %q not found", a.ConnectAs)
}

// toServer converts kernel permissions to the server's representation.
func (p *AuthPerms) toServer() *server.Permissions {
	if p == nil {
		return nil
	}
	perms := &server.Permissions{}
	if p.Publish != nil {
		perms.Publish = &server.SubjectPermission{Allow: p.Publish.Allow, Deny: p.Publish.Deny}
	}
	if p.Subscribe != nil {
		perms.Subscribe = &server.SubjectPermission{Allow: p.Subscribe.Allow, Deny: p.Subscribe.Deny}
	}
	return perms
}

// serverConfig builds a server-side TLS config, requiring client certificates.
func (t *TLSConfig) serverConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	if t.CAFile != "" {
		pool, err := loadCAPool(t.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
	}
	return cfg, nil
}

// clientConfig builds a client-side TLS config presenting a client cert.
func (t *TLSConfig) clientConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %w", err)
	}
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ServerName:         t.ServerName,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
	if t.CAFile != "" {
		pool, err := loadCAPool(t.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// loadCAPool reads a PEM bundle into a certificate pool.
func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca file %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca file %q contains no usable certificates", path)
	}
	return pool, nil
}

// Validate checks that the configuration is internally consistent.
func (a AuthConfig) Validate() error {
	switch a.Method {
	case "", AuthNone:
		return nil
	case AuthUserPass, AuthToken, AuthUsers, AuthNKey, AuthCreds, AuthTLS:
		return nil
	default:
		return fmt.Errorf("unknown auth method %q (want one of: %s)", a.Method,
			strings.Join([]string{
				string(AuthNone), string(AuthUserPass), string(AuthToken),
				string(AuthUsers), string(AuthNKey), string(AuthCreds), string(AuthTLS),
			}, ", "))
	}
}

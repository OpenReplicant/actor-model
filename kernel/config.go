package kernel

import (
	"encoding/json"
	"os"
	"time"
)

// Duration is a time.Duration that unmarshals from Go duration strings
// (e.g. "1h", "30m", "5s") in JSON config files.
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler for Duration.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON implements json.Marshaler for Duration.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// pluginFactory creates an Actor from an actor config entry.
type pluginFactory func(id string, topics []string, cfg map[string]interface{}, reg Publisher) (Actor, error)

// pluginRegistry maps plugin names (echo, proc, rest, stdio, docker, ...) to
// their factory functions. It remains for convenience, but Instantiate prefers
// an explicit plugin list so a binary's actor set is explicit and testable.
var pluginRegistry = make(map[string]pluginFactory)

// RegisterPlugin registers a factory for a plugin name. Duplicate names replace
// the previous registration.
func RegisterPlugin(name string, f pluginFactory) {
	pluginRegistry[name] = f
}

// Plugin pairs a plugin name with its factory function. A binary passes an
// explicit []Plugin to Instantiate instead of relying on the global
// pluginRegistry, which keeps two binaries in the same process from colliding.
type Plugin struct {
	Name    string
	Factory pluginFactory
}

// RetentionPolicy is a JetStream stream retention policy.
type RetentionPolicy string

const (
	// LimitsRetention retains messages until limits (max age/bytes/msgs) are hit.
	LimitsRetention RetentionPolicy = "limits"
	// InterestRetention retains messages until all known consumers have acked them.
	InterestRetention RetentionPolicy = "interest"
	// WorkQueueRetention retains messages until any one worker acks them.
	WorkQueueRetention RetentionPolicy = "workqueue"
)

// TopicConfig describes a single topic subscription with optional JetStream
// durability. Plain topics use in-process NATS messaging (fast, ephemeral,
// drop-if-full mailbox). JetStream topics use a durable stream + consumer
// (replayable, at-least-once, backpressured instead of dropped).
type TopicConfig struct {
	Subject   string          `json:"subject"`
	JetStream bool            `json:"jetstream"`
	Retention RetentionPolicy `json:"retention,omitempty"`
	MaxAge    Duration        `json:"max_age,omitempty"`
	MaxBytes  int64           `json:"max_bytes,omitempty"`
	MaxMsgs   int64           `json:"max_msgs,omitempty"`
}

// Config is the top-level actor-kernel config.
type Config struct {
	Transport string        `json:"transport"`
	NATSURL   string        `json:"nats_url"`
	Actors    []ActorConfig `json:"actors"`
}

// ActorConfig describes a single actor to instantiate.
//
// Topics accepts either the legacy string form ("topics": ["a.b", "c.d"])
// where all topics are plain, or the object form
// ("topics": [{"subject": "a.b", "jetstream": true}, "c.d"]) where each topic
// can individually enable JetStream durability.
type ActorConfig struct {
	ID     string                 `json:"id"`
	Plugin string                 `json:"plugin"`
	Topics []TopicConfig          `json:"topics"`
	Config map[string]interface{} `json:"config"`
}

// topicConfigAlias is used for custom unmarshaling that accepts both string
// and object topic forms.
type topicConfigAlias TopicConfig

// UnmarshalJSON accepts either a string (plain topic) or an object (TopicConfig).
func (tc *TopicConfig) UnmarshalJSON(data []byte) error {
	// Try string form first.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		tc.Subject = s
		tc.JetStream = false
		return nil
	}
	// Fall back to object form.
	var alias topicConfigAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*tc = TopicConfig(alias)
	return nil
}

// UnmarshalJSON accepts both string and object topic forms for ActorConfig.
func (ac *ActorConfig) UnmarshalJSON(data []byte) error {
	type actorConfigAlias ActorConfig
	var alias actorConfigAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	// alias.Topics is already []TopicConfig which has custom UnmarshalJSON.
	*ac = ActorConfig(alias)
	return nil
}

// LoadConfig reads a JSON config file from path.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// UnknownPluginError is returned when a config references an unregistered plugin.
type UnknownPluginError struct {
	Plugin string
}

func (e *UnknownPluginError) Error() string {
	return "unknown plugin: " + e.Plugin
}

// Instantiate creates actors from a config using the given explicit plugin list.
// If plugins is nil, the global package-level pluginRegistry is used for
// backward compatibility. Returns the actors in config order.
func Instantiate(c Config, plugins []Plugin, reg Publisher) ([]Actor, error) {
	factories := pluginRegistry
	if plugins != nil {
		factories = make(map[string]pluginFactory, len(plugins))
		for _, p := range plugins {
			factories[p.Name] = p.Factory
		}
	}

	var actors []Actor
	for _, ac := range c.Actors {
		f, ok := factories[ac.Plugin]
		if !ok {
			return nil, &UnknownPluginError{Plugin: ac.Plugin}
		}
		topics := make([]string, 0, len(ac.Topics))
		for _, t := range ac.Topics {
			topics = append(topics, t.Subject)
		}
		a, err := f(ac.ID, topics, ac.Config, reg)
		if err != nil {
			return nil, err
		}
		actors = append(actors, a)
	}
	return actors, nil
}

// StartAll starts every actor on the registry, returning the first error.
func StartAll(reg *Registry, actors []Actor) error {
	for _, a := range actors {
		if err := reg.Start(a); err != nil {
			return err
		}
	}
	return nil
}

// TopicConfigs returns the topic configurations for the given actor id from the
// registry's config store, if any. Returns nil if the actor was not configured
// via a config file.
func (r *Registry) TopicConfigs(id string) []TopicConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfgs, ok := r.actorTopicConfigs[id]
	if !ok {
		return nil
	}
	out := make([]TopicConfig, len(cfgs))
	copy(out, cfgs)
	return out
}

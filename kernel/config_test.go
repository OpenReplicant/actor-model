package kernel

import (
	"os"
	"testing"
	"time"
)

func TestConfigLoadAndInstantiate(t *testing.T) {
	plugins := []Plugin{
		{Name: "echo", Factory: func(id string, topics []string, cfg map[string]interface{}, reg Publisher) (Actor, error) {
			return NewEchoActor(id, topics, reg), nil
		}},
		{Name: "world-model", Factory: func(id string, topics []string, cfg map[string]interface{}, reg Publisher) (Actor, error) {
			return &recordingActor{id: id, topics: topics}, nil
		}},
	}

	cfg := Config{
		Transport: "inproc",
		Actors: []ActorConfig{
			{ID: "echo", Plugin: "echo", Topics: []TopicConfig{{Subject: "echo.>"}}},
			{ID: "world-model", Plugin: "world-model", Topics: []TopicConfig{{Subject: "world.model.set"}, {Subject: "world.model.get"}}},
		},
	}

	actors, err := Instantiate(cfg, plugins, nil)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	if len(actors) != 2 {
		t.Fatalf("expected 2 actors, got %d", len(actors))
	}
	if actors[0].ID() != "echo" || actors[1].ID() != "world-model" {
		t.Fatalf("unexpected actor order: %s, %s", actors[0].ID(), actors[1].ID())
	}
}

func TestConfigEndToEnd(t *testing.T) {
	plugins := []Plugin{
		{Name: "echo", Factory: func(id string, topics []string, cfg map[string]interface{}, reg Publisher) (Actor, error) {
			return NewEchoActor(id, topics, reg), nil
		}},
	}

	reg, cleanup := embeddedBus(t)
	defer cleanup()

	cfg := Config{
		Transport: "inproc",
		Actors: []ActorConfig{
			{ID: "echo", Plugin: "echo", Topics: []TopicConfig{{Subject: "echo.>"}}},
		},
	}
	actors, err := Instantiate(cfg, plugins, reg)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	if err := StartAll(reg, actors); err != nil {
		t.Fatalf("start all: %v", err)
	}

	reply, err := reg.Request("echo.from-config", []byte("config-works"), 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(reply) != "config-works" {
		t.Fatalf("expected echo, got %q", string(reply))
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	// write a temp config file
	content := `{"transport":"inproc","actors":[{"id":"echo","plugin":"echo","topics":["echo.>"]}]}`
	path := t.TempDir() + "/config.json"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Transport != "inproc" || len(c.Actors) != 1 || c.Actors[0].ID != "echo" {
		t.Fatalf("unexpected config: %+v", c)
	}
	if c.Actors[0].Topics[0].JetStream {
		t.Fatal("expected plain topic")
	}
	if c.Actors[0].Topics[0].Subject != "echo.>" {
		t.Fatalf("unexpected subject: %s", c.Actors[0].Topics[0].Subject)
	}
}

func TestLoadConfigFromFileWithJetStream(t *testing.T) {
	content := `{"transport":"inproc","actors":[{"id":"durable","plugin":"echo","topics":[{"subject":"durable.>","jetstream":true,"retention":"interest","max_age":"1h"}]}]}`
	path := t.TempDir() + "/config.json"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	tc := c.Actors[0].Topics[0]
	if !tc.JetStream {
		t.Fatal("expected jetstream topic")
	}
	if tc.Subject != "durable.>" {
		t.Fatalf("unexpected subject: %s", tc.Subject)
	}
	if tc.Retention != InterestRetention {
		t.Fatalf("unexpected retention: %s", tc.Retention)
	}
	if tc.MaxAge != Duration(time.Hour) {
		t.Fatalf("unexpected max age: %v", tc.MaxAge)
	}
}

func TestInstantiateUnknownPlugin(t *testing.T) {
	cfg := Config{Actors: []ActorConfig{{ID: "x", Plugin: "no-such-plugin"}}}
	_, err := Instantiate(cfg, nil, nil)
	if err == nil {
		t.Fatal("expected error for unknown plugin")
	}
	if _, ok := err.(*UnknownPluginError); !ok {
		t.Fatalf("expected UnknownPluginError, got %T", err)
	}
}

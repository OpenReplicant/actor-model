package kernel

import (
	"os"
	"testing"
	"time"
)

// TestJetStreamPlainAndDurable verifies both plain and JetStream topics work:
//   - plain topics: fast, ephemeral (current default)
//   - jetstream topics: durable stream + consumer (per-topic toggle)
func TestJetStreamPlainAndDurable(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	// Store topic configs: "durable.echo" is JetStream-backed, "plain.echo" is not.
	reg.StoreTopicConfigs("js-test", []TopicConfig{
		{Subject: "durable.echo", JetStream: true},
		{Subject: "plain.echo", JetStream: false},
	})

	a := &recordingActor{id: "js-test", topics: []string{"durable.echo", "plain.echo"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Publish to both topics.
	for i := 0; i < 5; i++ {
		if err := reg.Publish("plain.echo", []byte("plain")); err != nil {
			t.Fatalf("publish plain: %v", err)
		}
		if err := reg.Publish("durable.echo", []byte("durable")); err != nil {
			t.Fatalf("publish durable: %v", err)
		}
	}

	if !waitFor(t, 5*time.Second, func() bool { return a.count() >= 10 }) {
		t.Fatalf("expected >=10 handles, got %d", a.count())
	}
}

// TestJetStreamDurableSurvivesRestart verifies that a JetStream durable
// consumer is re-established on actor restart (the stream/consumer persist
// in JetStream, unlike plain in-memory mailboxes which are lost).
func TestJetStreamDurableSurvivesRestart(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reg.StoreTopicConfigs("js-restart", []TopicConfig{
		{Subject: "js-restart.echo", JetStream: true},
	})

	a := &recordingActor{id: "js-restart", topics: []string{"js-restart.echo"}}
	if err := reg.Start(a); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := reg.Publish("js-restart.echo", []byte("before-restart")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return a.count() == 1 }) {
		t.Fatalf("expected 1 handle before restart, got %d", a.count())
	}

	// Stop the actor (delivery goroutine exits, JS consumer unsubscribes).
	if err := reg.Stop(a); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Restart the same actor id: the durable JetStream consumer is re-attached.
	a2 := &recordingActor{id: "js-restart", topics: []string{"js-restart.echo"}}
	if err := reg.Start(a2); err != nil {
		t.Fatalf("restart: %v", err)
	}

	// Publish after restart — the durable consumer should still receive.
	if err := reg.Publish("js-restart.echo", []byte("after-restart")); err != nil {
		t.Fatalf("publish after restart: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return a2.count() == 1 }) {
		t.Fatalf("expected handle after restart, got %d", a2.count())
	}
}

// TestJetStreamBacklogReplayedOnStart is the core durability promise: messages
// published while the actor is stopped are still in the stream, and the durable
// consumer delivers them once the actor starts again. A plain topic in the same
// actor loses its messages by contrast.
func TestJetStreamBacklogReplayedOnStart(t *testing.T) {
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reg.StoreTopicConfigs("backlog", []TopicConfig{
		{Subject: "backlog.durable", JetStream: true},
		{Subject: "backlog.plain", JetStream: false},
	})

	// First incarnation: start so the stream + consumer exist, then stop.
	first := &recordingActor{id: "backlog", topics: []string{"backlog.durable", "backlog.plain"}}
	if err := reg.Start(first); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := reg.Stop(first); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Publish while the actor is down. Plain topics are fire-and-forget with no
	// subscriber; the durable subject lands in the stream.
	js, err := reg.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := js.Publish("backlog.durable", []byte("queued")); err != nil {
			t.Fatalf("js publish: %v", err)
		}
		if err := reg.Publish("backlog.plain", []byte("lost")); err != nil {
			t.Fatalf("plain publish: %v", err)
		}
	}

	// Second incarnation: the durable consumer replays the backlog. Only the
	// three durable messages arrive; the three plain ones were never stored.
	second := &recordingActor{id: "backlog", topics: []string{"backlog.durable", "backlog.plain"}}
	if err := reg.Start(second); err != nil {
		t.Fatalf("restart: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool { return second.count() >= 3 }) {
		t.Fatalf("expected 3 replayed messages, got %d", second.count())
	}
	for _, s := range second.subjects() {
		if s != "backlog.durable" {
			t.Fatalf("unexpected replayed subject %q (plain topics must not replay)", s)
		}
	}
}

// TestJetStreamConfigLoad verifies end-to-end config loading of JetStream topics.
func TestJetStreamConfigLoad(t *testing.T) {
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
	if tc.Retention != InterestRetention {
		t.Fatalf("unexpected retention: %s", tc.Retention)
	}
	if tc.MaxAge != Duration(time.Hour) {
		t.Fatalf("unexpected max age: %v", tc.MaxAge)
	}

	// End-to-end: start the durable echo actor on the embedded bus.
	reg, cleanup := embeddedBus(t)
	defer cleanup()

	reg.StoreTopicConfigs("durable", c.Actors[0].Topics)

	echo := &recordingActor{id: "durable", topics: []string{"durable.>"}}
	if err := reg.Start(echo); err != nil {
		t.Fatalf("start durable: %v", err)
	}

	// Publish directly to the JetStream stream via the registry's JS context.
	js, err := reg.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.Publish("durable.test", []byte("js works")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool { return echo.count() >= 1 }) {
		t.Fatalf("expected durable echo to handle message, got %d", echo.count())
	}
}

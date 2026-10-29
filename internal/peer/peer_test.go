package peer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"p2pchat/internal/protocol"
	"p2pchat/internal/relay"
)

func startTestRelay(t *testing.T, secret string) (context.Context, string) {
	t.Helper()
	r := relay.New(relay.Config{
		Addr:        "127.0.0.1:0",
		Secret:      secret,
		Timeout:     3 * time.Second,
		JoinTimeout: 3 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = r.Close()
	})

	deadline := time.Now().Add(2 * time.Second)
	for r.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if r.Addr() == "" {
		t.Fatal("relay did not start")
	}
	return ctx, r.Addr()
}

func startPair(t *testing.T) (*Peer, *Peer) {
	return startPairSecret(t, "")
}

func startPairSecret(t *testing.T, secret string) (*Peer, *Peer) {
	t.Helper()
	t.Chdir(t.TempDir())
	ctx, addr := startTestRelay(t, secret)

	alice := New(Config{RelayAddr: addr, Name: "alice", Secret: secret, DialTimeout: time.Second})
	if err := alice.Start(ctx); err != nil {
		t.Fatalf("alice start: %v", err)
	}
	t.Cleanup(alice.Close)

	bob := New(Config{RelayAddr: addr, Name: "bob", Secret: secret, DialTimeout: time.Second})
	if err := bob.Start(ctx); err != nil {
		t.Fatalf("bob start: %v", err)
	}
	t.Cleanup(bob.Close)

	alice.refreshPeers()
	bob.refreshPeers()
	waitEvent(t, alice.Events(), func(e Event) bool {
		return e.Kind == EventPeers && hasPeer(e.Peers, "bob")
	}, 3*time.Second)
	waitEvent(t, bob.Events(), func(e Event) bool {
		return e.Kind == EventPeers && hasPeer(e.Peers, "alice")
	}, 3*time.Second)

	return alice, bob
}

func hasPeer(peers []protocol.PeerInfo, name string) bool {
	for _, p := range peers {
		if p.Name == name {
			return true
		}
	}
	return false
}

func waitEvent(t *testing.T, ch <-chan Event, match func(Event) bool, timeout time.Duration) Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			if match(e) {
				return e
			}
		case <-deadline:
			t.Fatal("timed out waiting for event")
		}
	}
}

func connected(e Event) bool {
	return e.Kind == EventSession && e.Text == "connected"
}

func TestDirectChatAndFile(t *testing.T) {
	alice, bob := startPair(t)

	alice.Connect("bob")
	ev := waitEvent(t, alice.Events(), connected, 5*time.Second)
	if ev.Conn != Direct {
		t.Fatalf("alice connection = %s, want DIRECT", ev.Conn)
	}
	ev = waitEvent(t, bob.Events(), connected, 5*time.Second)
	if ev.Conn != Direct {
		t.Fatalf("bob connection = %s, want DIRECT", ev.Conn)
	}

	if err := alice.SendChat("hello bob"); err != nil {
		t.Fatal(err)
	}
	chat := waitEvent(t, bob.Events(), func(e Event) bool {
		return e.Kind == EventChat && e.Text == "hello bob"
	}, 5*time.Second)
	if chat.Peer != "alice" {
		t.Fatalf("chat from %q", chat.Peer)
	}

	data := bytes.Repeat([]byte("0123456789abcdef"), 8192)
	if err := os.WriteFile("payload.bin", data, 0o600); err != nil {
		t.Fatal(err)
	}
	alice.SendFile("payload.bin")
	done := waitEvent(t, bob.Events(), func(e Event) bool {
		return e.Kind == EventTransfer && e.Done
	}, 10*time.Second)
	if !done.OK {
		t.Fatal("receiver reported sha256 failure")
	}

	matches, err := filepath.Glob("recv-*-payload.bin")
	if err != nil || len(matches) != 1 {
		t.Fatalf("received files = %v, err = %v", matches, err)
	}
	got, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("file content mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestRelayFallback(t *testing.T) {
	alice, bob := startPair(t)

	// Break the direct path but leave the relay control connection intact.
	bob.mu.Lock()
	_ = bob.listener.Close()
	bob.mu.Unlock()

	alice.Connect("bob")
	ev := waitEvent(t, alice.Events(), connected, 5*time.Second)
	if ev.Conn != Relayed {
		t.Fatalf("alice connection = %s, want RELAYED", ev.Conn)
	}
	ev = waitEvent(t, bob.Events(), connected, 5*time.Second)
	if ev.Conn != Relayed {
		t.Fatalf("bob connection = %s, want RELAYED", ev.Conn)
	}

	if err := alice.SendChat("through the relay"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, bob.Events(), func(e Event) bool {
		return e.Kind == EventChat && e.Text == "through the relay"
	}, 5*time.Second)
}

func TestSecretAuthWired(t *testing.T) {
	alice, bob := startPairSecret(t, "shared-secret")

	alice.Connect("bob")
	waitEvent(t, alice.Events(), connected, 5*time.Second)
	waitEvent(t, bob.Events(), connected, 5*time.Second)

	if err := alice.SendChat("authenticated hello"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, bob.Events(), func(e Event) bool {
		return e.Kind == EventChat && e.Text == "authenticated hello"
	}, 5*time.Second)
}

func TestWrongSecretRejected(t *testing.T) {
	t.Chdir(t.TempDir())
	ctx, addr := startTestRelay(t, "right-secret")

	p := New(Config{RelayAddr: addr, Name: "eve", Secret: "wrong-secret", DialTimeout: time.Second})
	if err := p.Start(ctx); err == nil {
		p.Close()
		t.Fatal("peer with the wrong secret was accepted")
	}
}

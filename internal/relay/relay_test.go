package relay

import (
	"context"
	"net"
	"testing"
	"time"

	"p2pchat/internal/protocol"
)

func startRelay(t *testing.T) (*Relay, string) {
	t.Helper()
	r := New(Config{Addr: "127.0.0.1:0", Timeout: 3 * time.Second, JoinTimeout: 2 * time.Second})
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
	return r, r.Addr()
}

func register(t *testing.T, addr, name, dataAddr string) *protocol.Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	c := protocol.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if err := c.Send(protocol.Message{Type: protocol.Hello, Name: name, Addr: dataAddr}); err != nil {
		t.Fatalf("hello: %v", err)
	}
	var m protocol.Message
	if err := c.Recv(&m); err != nil {
		t.Fatalf("welcome: %v", err)
	}
	if m.Type != protocol.Welcome {
		t.Fatalf("welcome type = %q", m.Type)
	}
	return c
}

func TestRelayPairsTwoPeers(t *testing.T) {
	_, addr := startRelay(t)

	_ = register(t, addr, "alice", ":40001")
	bobCtrl := register(t, addr, "bob", ":40002")

	sid := protocol.NewChallenge()

	alice, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ac := protocol.NewConn(alice)
	t.Cleanup(func() { _ = ac.Close() })
	_ = ac.SetDeadline(time.Now().Add(2 * time.Second))
	if err := ac.Send(protocol.Message{Type: protocol.RelayJoin, Session: sid, From: "alice", To: "bob"}); err != nil {
		t.Fatal(err)
	}

	// bob learns about the session on its control connection
	var offer protocol.Message
	for {
		if err := bobCtrl.Recv(&offer); err != nil {
			t.Fatalf("waiting for offer: %v", err)
		}
		if offer.Type == protocol.RelayOffer {
			break
		}
	}
	if offer.Session != sid || offer.From != "alice" {
		t.Fatalf("offer = %+v", offer)
	}

	bob, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	bc := protocol.NewConn(bob)
	t.Cleanup(func() { _ = bc.Close() })
	_ = bc.SetDeadline(time.Now().Add(2 * time.Second))
	if err := bc.Send(protocol.Message{Type: protocol.RelayJoin, Session: sid, From: "bob", To: "alice"}); err != nil {
		t.Fatal(err)
	}

	var ready protocol.Message
	if err := ac.Recv(&ready); err != nil {
		t.Fatalf("alice ready: %v", err)
	}
	if ready.Type != protocol.RelayReady {
		t.Fatalf("alice ready type = %q", ready.Type)
	}
	if err := bc.Recv(&ready); err != nil {
		t.Fatalf("bob ready: %v", err)
	}
	if ready.Type != protocol.RelayReady {
		t.Fatalf("bob ready type = %q", ready.Type)
	}

	_ = ac.SetDeadline(time.Now().Add(2 * time.Second))
	_ = bc.SetDeadline(time.Now().Add(2 * time.Second))
	if err := ac.Send(protocol.DataMessage{Type: protocol.DataChat, Text: "hello bob"}); err != nil {
		t.Fatal(err)
	}
	var got protocol.DataMessage
	if err := bc.Recv(&got); err != nil {
		t.Fatalf("bob recv: %v", err)
	}
	if got.Text != "hello bob" {
		t.Fatalf("got %q", got.Text)
	}
	if err := bc.Send(protocol.DataMessage{Type: protocol.DataChat, Text: "hello alice"}); err != nil {
		t.Fatal(err)
	}
	if err := ac.Recv(&got); err != nil {
		t.Fatalf("alice recv: %v", err)
	}
	if got.Text != "hello alice" {
		t.Fatalf("got %q", got.Text)
	}
}

func TestRelayRejectsUnknownPeer(t *testing.T) {
	_, addr := startRelay(t)
	register(t, addr, "alice", ":40001")

	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := protocol.NewConn(nc)
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if err := c.Send(protocol.Message{Type: protocol.RelayJoin, Session: "x", From: "alice", To: "nobody"}); err != nil {
		t.Fatal(err)
	}
	var m protocol.Message
	if err := c.Recv(&m); err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.Error {
		t.Fatalf("type = %q, want error", m.Type)
	}
}

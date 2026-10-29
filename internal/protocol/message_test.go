package protocol

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

func TestFrameRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ca, cb := NewConn(a), NewConn(b)

	want := Message{Type: Hello, Name: "alice", Addr: ":1234"}
	sent := make(chan error, 1)
	go func() { sent <- ca.Send(want) }()

	var got Message
	if err := cb.Recv(&got); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if err := <-sent; err != nil {
		t.Fatalf("send: %v", err)
	}
	if got.Type != want.Type || got.Name != want.Name || got.Addr != want.Addr {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestMultipleFrames(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ca, cb := NewConn(a), NewConn(b)

	go func() {
		_ = ca.Send(Message{Type: List})
		_ = ca.Send(Message{Type: Peers, Peers: []PeerInfo{{Name: "bob", Addr: "1.2.3.4:9"}}})
	}()

	var first Message
	if err := cb.Recv(&first); err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Type != List {
		t.Fatalf("first type = %q", first.Type)
	}
	var second Message
	if err := cb.Recv(&second); err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(second.Peers) != 1 || second.Peers[0].Name != "bob" {
		t.Fatalf("second = %+v", second)
	}
}

func TestFrameTooLarge(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Write(bytes.Repeat([]byte("a"), MaxFrameSize+10))
		_, _ = a.Write([]byte{0})
	}()

	var m Message
	err := NewConn(b).Recv(&m)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	_ = b.Close()
	<-done
}

func TestBinaryDataRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ca, cb := NewConn(a), NewConn(b)

	payload := []byte{0x00, 0x01, 0xff, 0x00, 0x42}
	sent := make(chan error, 1)
	go func() {
		sent <- ca.Send(DataMessage{Type: DataFileChunk, Offset: 32 << 10, Data: payload})
	}()

	var got DataMessage
	if err := cb.Recv(&got); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if err := <-sent; err != nil {
		t.Fatalf("send: %v", err)
	}
	if got.Type != DataFileChunk || got.Offset != 32<<10 || !bytes.Equal(got.Data, payload) {
		t.Fatalf("got %+v", got)
	}
}

func TestDeadlineUnblocksRecv(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := NewConn(b)
	_ = c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))

	var m Message
	if err := c.Recv(&m); err == nil {
		t.Fatal("expected timeout error")
	}
}

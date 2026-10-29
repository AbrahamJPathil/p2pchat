// Package protocol defines the null-delimited JSON wire format shared by the
// relay, peers, and data sessions. The framing idea is borrowed from bore
// (https://github.com/ekzhang/bore, MIT).
package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	// DefaultRelayPort is the control port used by the relay.
	DefaultRelayPort = 7835

	// MaxFrameSize bounds a single JSON frame so a hostile stream cannot make
	// the reader allocate without limit. A 32 KiB file chunk base64-encodes to
	// roughly 44 KiB, which fits comfortably.
	MaxFrameSize = 64 << 10

	// DefaultTimeout is used for dials and handshake steps.
	DefaultTimeout = 5 * time.Second
)

// ErrFrameTooLarge is returned when a peer sends a frame above MaxFrameSize.
var ErrFrameTooLarge = errors.New("protocol: frame exceeds maximum size")

// Message types used on the relay control connection.
type Type string

const (
	Hello        Type = "hello"
	Welcome      Type = "welcome"
	List         Type = "list"
	Peers        Type = "peers"
	Heartbeat    Type = "heartbeat"
	Ping         Type = "ping"
	RelayJoin    Type = "relay_join"
	RelayOffer   Type = "relay_offer"
	RelayReady   Type = "relay_ready"
	Challenge    Type = "challenge"
	Authenticate Type = "authenticate"
	Error        Type = "error"
)

// PeerInfo is one entry in the relay's live peer list.
type PeerInfo struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// Message is the single envelope used for relay control traffic and for the
// auth challenge/response. Unused fields are omitted on the wire.
type Message struct {
	Type      Type       `json:"type"`
	Name      string     `json:"name,omitempty"`
	Addr      string     `json:"addr,omitempty"`
	Peers     []PeerInfo `json:"peers,omitempty"`
	Session   string     `json:"session,omitempty"`
	From      string     `json:"from,omitempty"`
	To        string     `json:"to,omitempty"`
	Challenge string     `json:"challenge,omitempty"`
	Answer    string     `json:"answer,omitempty"`
	Error     string     `json:"error,omitempty"`
}

// Conn is a TCP connection carrying null-delimited JSON frames. Writes are
// serialized so several goroutines may send safely.
type Conn struct {
	c  net.Conn
	r  *bufio.Reader
	mu sync.Mutex
}

// NewConn wraps a net.Conn with the JSON framing.
func NewConn(c net.Conn) *Conn {
	return &Conn{c: c, r: bufio.NewReaderSize(c, MaxFrameSize+1)}
}

// Send marshals v, appends the null delimiter, and writes it in one call.
func (c *Conn) Send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	b = append(b, 0)

	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = c.c.Write(b)
	return err
}

// Recv reads one null-delimited frame and unmarshals it into v.
func (c *Conn) Recv(v any) error {
	b, err := c.r.ReadSlice(0)
	if errors.Is(err, bufio.ErrBufferFull) {
		return ErrFrameTooLarge
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b[:len(b)-1], v); err != nil {
		return fmt.Errorf("protocol: decode frame: %w", err)
	}
	return nil
}

// SetDeadline sets both read and write deadlines on the underlying socket.
func (c *Conn) SetDeadline(t time.Time) error { return c.c.SetDeadline(t) }

// SetReadDeadline sets only the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.c.SetReadDeadline(t) }

// Close closes the underlying socket.
func (c *Conn) Close() error { return c.c.Close() }

// RemoteAddr reports the peer address.
func (c *Conn) RemoteAddr() net.Addr { return c.c.RemoteAddr() }

// Detach returns the raw socket plus any bytes already buffered by the frame
// reader. It is used when a control connection becomes a raw byte proxy.
func (c *Conn) Detach() (net.Conn, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.r.Buffered()
	if n == 0 {
		return c.c, nil
	}
	rest, _ := c.r.Peek(n)
	out := append([]byte(nil), rest...)
	_, _ = c.r.Discard(n)
	return c.c, out
}

// Data message types used between two peers over an established session.
type DataType string

const (
	DataHello     DataType = "hello"
	DataChat      DataType = "chat"
	DataPing      DataType = "ping"
	DataPong      DataType = "pong"
	DataFileStart DataType = "file_start"
	DataFileChunk DataType = "file_chunk"
	DataFileEnd   DataType = "file_end"
	DataFileAck   DataType = "file_ack"
	DataBye       DataType = "bye"
)

// DataMessage is the envelope for chat, heartbeat, and file transfer frames.
// Data is base64-encoded automatically by encoding/json.
type DataMessage struct {
	Type   DataType `json:"type"`
	Name   string   `json:"name,omitempty"`
	Text   string   `json:"text,omitempty"`
	Size   int64    `json:"size,omitempty"`
	Offset int64    `json:"offset,omitempty"`
	Data   []byte   `json:"data,omitempty"`
	SHA256 string   `json:"sha256,omitempty"`
	TS     int64    `json:"ts,omitempty"`
	OK     bool     `json:"ok,omitempty"`
	Error  string   `json:"error,omitempty"`
}

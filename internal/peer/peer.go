// Package peer implements the client side: relay registration, discovery,
// direct/relayed connection setup, chat, and file transfer.
package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"p2pchat/internal/protocol"
)

// ConnType is shown in the TUI so the demo can distinguish the two paths.
type ConnType string

const (
	Direct  ConnType = "DIRECT"
	Relayed ConnType = "RELAYED"
)

// EventKind tags asynchronous updates sent from the engine to the TUI.
type EventKind string

const (
	EventStatus   EventKind = "status"
	EventPeers    EventKind = "peers"
	EventChat     EventKind = "chat"
	EventSession  EventKind = "session"
	EventStats    EventKind = "stats"
	EventTransfer EventKind = "transfer"
	EventError    EventKind = "error"
)

// Event is a single engine update.
type Event struct {
	Kind     EventKind
	Text     string
	Peer     string
	Peers    []protocol.PeerInfo
	Conn     ConnType
	RTT      time.Duration
	Sent     int64
	Recv     int64
	File     string
	Progress int64
	Size     int64
	Done     bool
	OK       bool
	Err      error
}

// Config controls a peer.
type Config struct {
	RelayAddr   string
	Name        string
	Secret      string
	Timeout     time.Duration
	DialTimeout time.Duration
	Logf        func(format string, args ...any)
}

// Peer is a running peer engine. It is safe to call its exported methods from
// the TUI goroutine.
type Peer struct {
	cfg    Config
	events chan Event

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	closed   bool
	ctrl     *protocol.Conn
	listener net.Listener
	peers    []protocol.PeerInfo
	session  *Session
}

// New creates a peer. Start must be called before using it.
func New(cfg Config) *Peer {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 2 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Peer{cfg: cfg, events: make(chan Event, 256)}
}

// Events returns the channel the TUI subscribes to.
func (p *Peer) Events() <-chan Event { return p.events }

// Start listens for direct connections, registers with the relay, and starts
// the control/heartbeat loops.
func (p *Peer) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return fmt.Errorf("listen for direct peers: %w", err)
	}
	dataAddr := ":" + strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)

	c, err := p.dialRelay()
	if err != nil {
		_ = ln.Close()
		return err
	}
	_ = c.SetDeadline(time.Now().Add(protocol.DefaultTimeout))
	if err := c.Send(protocol.Message{Type: protocol.Hello, Name: p.cfg.Name, Addr: dataAddr}); err != nil {
		_ = c.Close()
		_ = ln.Close()
		return fmt.Errorf("register: %w", err)
	}
	var m protocol.Message
	if err := c.Recv(&m); err != nil {
		_ = c.Close()
		_ = ln.Close()
		return fmt.Errorf("register: %w", err)
	}
	if m.Type == protocol.Error {
		_ = c.Close()
		_ = ln.Close()
		return fmt.Errorf("relay: %s", m.Error)
	}
	if m.Type == protocol.Challenge {
		_ = c.Close()
		_ = ln.Close()
		return errors.New("relay requires a secret (use --secret)")
	}
	if m.Type != protocol.Welcome {
		_ = c.Close()
		_ = ln.Close()
		return fmt.Errorf("relay: unexpected %q", m.Type)
	}
	_ = c.SetDeadline(time.Time{})

	p.mu.Lock()
	p.ctrl = c
	p.listener = ln
	p.mu.Unlock()

	p.status("registered as %s via %s", p.cfg.Name, p.cfg.RelayAddr)
	go p.ctrlLoop(c)
	go p.heartbeatLoop()
	go p.acceptLoop(ln)
	p.refreshPeers()
	return nil
}

// Close shuts down the relay connection, listener, and active session.
func (p *Peer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	cancel := p.cancel
	ctrl := p.ctrl
	ln := p.listener
	s := p.session
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ctrl != nil {
		_ = ctrl.Close()
	}
	if ln != nil {
		_ = ln.Close()
	}
	if s != nil {
		s.close()
	}
}

// Connect starts an asynchronous attempt to reach name, trying DIRECT first
// and falling back to RELAYED.
func (p *Peer) Connect(name string) {
	go p.connect(name)
}

// SendChat sends a text message over the active session.
func (p *Peer) SendChat(text string) error {
	s := p.currentSession()
	if s == nil {
		return errors.New("not connected to a peer")
	}
	if !s.send(protocol.DataMessage{Type: protocol.DataChat, Text: text}) {
		return errors.New("session closed")
	}
	s.sent.Add(int64(len(text)))
	s.stats()
	return nil
}

// SendFile starts an asynchronous file transfer over the active session.
func (p *Peer) SendFile(path string) {
	go func() {
		s := p.currentSession()
		if s == nil {
			p.fail(errors.New("not connected to a peer"))
			return
		}
		if err := s.sendFile(path); err != nil {
			p.fail(err)
		}
	}()
}

func (p *Peer) ctrlLoop(c *protocol.Conn) {
	for {
		_ = c.SetReadDeadline(time.Now().Add(p.cfg.Timeout))
		var m protocol.Message
		if err := c.Recv(&m); err != nil {
			if p.ctx.Err() != nil || p.isClosed() {
				return
			}
			p.fail(fmt.Errorf("relay connection lost: %w", err))
			p.Close()
			return
		}
		switch m.Type {
		case protocol.Heartbeat:
			_ = p.sendCtrl(protocol.Message{Type: protocol.Ping})
		case protocol.Peers:
			p.setPeers(m.Peers)
		case protocol.RelayOffer:
			if p.sessionActive() {
				p.status("ignoring relay offer from %s (already connected)", m.From)
				continue
			}
			go p.joinRelayed(m.Session, m.From)
		case protocol.Error:
			p.fail(fmt.Errorf("relay: %s", m.Error))
		}
	}
}

func (p *Peer) heartbeatLoop() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			if err := p.sendCtrl(protocol.Message{Type: protocol.Ping}); err != nil {
				return
			}
			p.refreshPeers()
		}
	}
}

func (p *Peer) acceptLoop(ln net.Listener) {
	for {
		nc, err := ln.Accept()
		if err != nil {
			if p.ctx.Err() != nil || p.isClosed() {
				return
			}
			p.fail(fmt.Errorf("direct listener: %w", err))
			return
		}
		go p.handleIncoming(nc)
	}
}

func (p *Peer) handleIncoming(nc net.Conn) {
	c := protocol.NewConn(nc)
	ok := false
	defer func() {
		if !ok {
			_ = c.Close()
		}
	}()

	if p.cfg.Secret != "" {
		_ = c.SetDeadline(time.Now().Add(protocol.DefaultTimeout))
		if err := protocol.ServerHandshake(c, p.cfg.Secret); err != nil {
			return
		}
	}
	name, err := exchangeHello(c, p.cfg.Name)
	if err != nil {
		return
	}
	if err := p.startSession(c, name, Direct); err != nil {
		_ = c.Send(protocol.DataMessage{Type: protocol.DataBye, Error: "busy"})
		return
	}
	ok = true
}

func (p *Peer) dialRelay() (*protocol.Conn, error) {
	nc, err := net.DialTimeout("tcp", p.cfg.RelayAddr, p.cfg.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial relay %s: %w", p.cfg.RelayAddr, err)
	}
	c := protocol.NewConn(nc)
	if p.cfg.Secret != "" {
		_ = c.SetDeadline(time.Now().Add(protocol.DefaultTimeout))
		if err := protocol.ClientHandshake(c, p.cfg.Secret); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("relay auth: %w", err)
		}
	}
	_ = c.SetDeadline(time.Time{})
	return c, nil
}

func (p *Peer) sendCtrl(m protocol.Message) error {
	p.mu.Lock()
	c := p.ctrl
	p.mu.Unlock()
	if c == nil {
		return errors.New("relay not connected")
	}
	return c.Send(m)
}

func (p *Peer) refreshPeers() {
	_ = p.sendCtrl(protocol.Message{Type: protocol.List})
}

func (p *Peer) setPeers(peers []protocol.PeerInfo) {
	p.mu.Lock()
	p.peers = peers
	p.mu.Unlock()
	p.emit(Event{Kind: EventPeers, Peers: peers})
}

func (p *Peer) findPeer(name string) (protocol.PeerInfo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, x := range p.peers {
		if x.Name == name {
			return x, true
		}
	}
	return protocol.PeerInfo{}, false
}

func (p *Peer) currentSession() *Session {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session
}

func (p *Peer) sessionActive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session != nil
}

func (p *Peer) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *Peer) emit(e Event) {
	select {
	case p.events <- e:
	default:
	}
}

func (p *Peer) status(format string, args ...any) {
	p.emit(Event{Kind: EventStatus, Text: fmt.Sprintf(format, args...)})
}

func (p *Peer) fail(err error) {
	p.emit(Event{Kind: EventError, Err: err})
}

// Session is one established peer-to-peer conversation.
type Session struct {
	p    *Peer
	c    *protocol.Conn
	name string
	kind ConnType

	sendCh    chan protocol.DataMessage
	done      chan struct{}
	closeOnce sync.Once
	sent      atomic.Int64
	recv      atomic.Int64
	rtt       atomic.Int64

	incoming *incomingFile
	fileMu   sync.Mutex

	sendFileMu sync.Mutex

	statsMu    sync.Mutex
	lastStats  time.Time
	progressMu sync.Mutex
	lastProg   time.Time
}

func (p *Peer) startSession(c *protocol.Conn, name string, kind ConnType) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = c.Close()
		return errors.New("peer is closed")
	}
	if p.session != nil {
		p.mu.Unlock()
		return errors.New("already in a session")
	}
	s := &Session{
		p:      p,
		c:      c,
		name:   name,
		kind:   kind,
		sendCh: make(chan protocol.DataMessage, 64),
		done:   make(chan struct{}),
	}
	p.session = s
	p.mu.Unlock()

	p.emit(Event{Kind: EventSession, Peer: name, Conn: kind, Text: "connected"})
	go s.run()
	return nil
}

func (s *Session) run() {
	go s.writeLoop()
	s.readLoop()
	s.close()
}

func (s *Session) writeLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			ping := protocol.DataMessage{Type: protocol.DataPing, TS: time.Now().UnixNano()}
			if err := s.c.Send(ping); err != nil {
				s.close()
				return
			}
		case m := <-s.sendCh:
			if err := s.c.Send(m); err != nil {
				s.close()
				return
			}
			switch m.Type {
			case protocol.DataChat:
				s.sent.Add(int64(len(m.Text)))
			case protocol.DataFileChunk:
				s.sent.Add(int64(len(m.Data)))
			}
			s.stats()
		}
	}
}

func (s *Session) readLoop() {
	for {
		_ = s.c.SetReadDeadline(time.Now().Add(10 * time.Second))
		var m protocol.DataMessage
		if err := s.c.Recv(&m); err != nil {
			select {
			case <-s.done:
				return
			default:
			}
			s.p.fail(fmt.Errorf("session with %s: %w", s.name, err))
			return
		}
		switch m.Type {
		case protocol.DataHello:
		case protocol.DataChat:
			s.recv.Add(int64(len(m.Text)))
			s.p.emit(Event{Kind: EventChat, Peer: s.name, Text: m.Text})
			s.stats()
		case protocol.DataPing:
			_ = s.c.Send(protocol.DataMessage{Type: protocol.DataPong, TS: m.TS})
		case protocol.DataPong:
			s.rtt.Store(int64(time.Since(time.Unix(0, m.TS))))
			s.stats()
		case protocol.DataFileStart:
			if err := s.beginIncoming(m); err != nil {
				s.p.fail(err)
			}
		case protocol.DataFileChunk:
			if err := s.writeIncoming(m); err != nil {
				s.p.fail(err)
			}
		case protocol.DataFileEnd:
			if err := s.endIncoming(m); err != nil {
				s.p.fail(err)
			}
		case protocol.DataFileAck:
			s.p.emit(Event{Kind: EventTransfer, Peer: s.name, File: m.Name, Done: true, OK: m.OK, Text: m.Error})
			if m.OK {
				s.p.status("peer verified %s", m.Name)
			} else {
				s.p.status("peer rejected %s: %s", m.Name, m.Error)
			}
		case protocol.DataBye:
			if m.Error != "" {
				s.p.status("peer left: %s", m.Error)
			}
			return
		}
	}
}

func (s *Session) stats() {
	s.statsMu.Lock()
	if time.Since(s.lastStats) < 200*time.Millisecond {
		s.statsMu.Unlock()
		return
	}
	s.lastStats = time.Now()
	s.statsMu.Unlock()

	s.p.emit(Event{
		Kind: EventStats,
		Peer: s.name,
		Conn: s.kind,
		RTT:  time.Duration(s.rtt.Load()),
		Sent: s.sent.Load(),
		Recv: s.recv.Load(),
	})
}

// progress throttles transfer updates so a large file cannot flood the TUI.
func (s *Session) progress(e Event) {
	s.progressMu.Lock()
	if time.Since(s.lastProg) < 100*time.Millisecond {
		s.progressMu.Unlock()
		return
	}
	s.lastProg = time.Now()
	s.progressMu.Unlock()
	s.p.emit(e)
}

func (s *Session) send(m protocol.DataMessage) bool {
	select {
	case s.sendCh <- m:
		return true
	case <-s.done:
		return false
	}
}

func (s *Session) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.c.Close()
		s.fileMu.Lock()
		if s.incoming != nil {
			_ = s.incoming.f.Close()
			s.incoming = nil
		}
		s.fileMu.Unlock()
		s.p.mu.Lock()
		if s.p.session == s {
			s.p.session = nil
		}
		s.p.mu.Unlock()
		s.p.emit(Event{Kind: EventSession, Peer: s.name, Conn: s.kind, Text: "disconnected"})
	})
}

func exchangeHello(c *protocol.Conn, name string) (string, error) {
	_ = c.SetDeadline(time.Now().Add(protocol.DefaultTimeout))
	if err := c.Send(protocol.DataMessage{Type: protocol.DataHello, Name: name}); err != nil {
		return "", err
	}
	var m protocol.DataMessage
	if err := c.Recv(&m); err != nil {
		return "", err
	}
	if m.Type != protocol.DataHello {
		return "", fmt.Errorf("peer: expected hello, got %q", m.Type)
	}
	_ = c.SetDeadline(time.Time{})
	return m.Name, nil
}

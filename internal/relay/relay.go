// Package relay implements the rendezvous server: it tracks live peers,
// answers discovery requests, and pairs two peers' byte streams when a direct
// connection is not possible. The pairing/proxy idea follows bore's server.rs.
package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"p2pchat/internal/protocol"
)

// Config controls the relay.
type Config struct {
	Addr        string
	Secret      string
	Timeout     time.Duration
	JoinTimeout time.Duration
	Logf        func(format string, args ...any)
}

// Relay is a running rendezvous server.
type Relay struct {
	cfg Config

	mu       sync.Mutex
	ln       net.Listener
	peers    map[string]*peerEntry
	sessions map[string]*pendingSession
}

type peerEntry struct {
	info protocol.PeerInfo
	ctrl *protocol.Conn
}

type pendingSession struct {
	initiator string
	target    string
	a         *protocol.Conn
	b         *protocol.Conn
	done      chan struct{}
	doneOnce  sync.Once
}

func (s *pendingSession) finish() { s.doneOnce.Do(func() { close(s.done) }) }

// New creates a relay. Empty config values get defaults.
func New(cfg Config) *Relay {
	if cfg.Addr == "" {
		cfg.Addr = ":7835"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.JoinTimeout <= 0 {
		cfg.JoinTimeout = 10 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Relay{
		cfg:      cfg,
		peers:    make(map[string]*peerEntry),
		sessions: make(map[string]*pendingSession),
	}
}

// Run listens until ctx is cancelled or Close is called.
func (r *Relay) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", r.cfg.Addr)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.ln = ln
	r.mu.Unlock()
	r.cfg.Logf("relay listening on %s", ln.Addr())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		nc, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go r.handle(ctx, nc)
	}
}

// Close stops the listener.
func (r *Relay) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ln == nil {
		return nil
	}
	return r.ln.Close()
}

// Addr returns the bound listen address, or "" before Run has started.
func (r *Relay) Addr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ln == nil {
		return ""
	}
	return r.ln.Addr().String()
}

func (r *Relay) handle(ctx context.Context, nc net.Conn) {
	c := protocol.NewConn(nc)
	owned := false
	defer func() {
		if !owned {
			_ = c.Close()
		}
	}()

	if r.cfg.Secret != "" {
		_ = c.SetDeadline(time.Now().Add(protocol.DefaultTimeout))
		if err := protocol.ServerHandshake(c, r.cfg.Secret); err != nil {
			_ = c.Send(protocol.Message{Type: protocol.Error, Error: "authentication failed"})
			return
		}
		_ = c.SetDeadline(time.Time{})
	}

	_ = c.SetReadDeadline(time.Now().Add(protocol.DefaultTimeout))
	var m protocol.Message
	if err := c.Recv(&m); err != nil {
		return
	}
	_ = c.SetReadDeadline(time.Time{})

	switch m.Type {
	case protocol.Hello:
		r.handlePeer(ctx, c, m)
	case protocol.List:
		r.sendPeers(c)
	case protocol.RelayJoin:
		owned = true
		r.handleJoin(c, m)
	default:
		_ = c.Send(protocol.Message{Type: protocol.Error, Error: "expected hello, list, or relay_join"})
	}
}

func (r *Relay) handlePeer(ctx context.Context, c *protocol.Conn, m protocol.Message) {
	name := strings.TrimSpace(m.Name)
	if name == "" {
		_ = c.Send(protocol.Message{Type: protocol.Error, Error: "name required"})
		return
	}
	_, port, err := net.SplitHostPort(m.Addr)
	if err != nil {
		_ = c.Send(protocol.Message{Type: protocol.Error, Error: "invalid data address"})
		return
	}
	observed := "127.0.0.1"
	if tcp, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		observed = tcp.IP.String()
	}
	publicAddr := net.JoinHostPort(observed, port)

	entry := &peerEntry{
		info: protocol.PeerInfo{Name: name, Addr: publicAddr},
		ctrl: c,
	}
	r.mu.Lock()
	if old := r.peers[name]; old != nil {
		_ = old.ctrl.Close()
	}
	r.peers[name] = entry
	r.mu.Unlock()
	r.cfg.Logf("peer %q registered at %s", name, publicAddr)
	defer func() {
		r.mu.Lock()
		if cur := r.peers[name]; cur != nil && cur.ctrl == c {
			delete(r.peers, name)
		}
		r.mu.Unlock()
		r.cfg.Logf("peer %q disconnected", name)
	}()

	if err := c.Send(protocol.Message{Type: protocol.Welcome, Name: name, Addr: publicAddr}); err != nil {
		return
	}

	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := c.Send(protocol.Message{Type: protocol.Heartbeat}); err != nil {
					_ = c.Close()
					return
				}
			}
		}
	}()

	for {
		_ = c.SetReadDeadline(time.Now().Add(r.cfg.Timeout))
		var msg protocol.Message
		if err := c.Recv(&msg); err != nil {
			return
		}
		switch msg.Type {
		case protocol.Ping:
			// Any frame proves liveness; deadline above is refreshed.
		case protocol.List:
			r.sendPeers(c)
		default:
			// Ignore unexpected control frames instead of dropping the peer.
		}
	}
}

func (r *Relay) sendPeers(c *protocol.Conn) {
	r.mu.Lock()
	peers := make([]protocol.PeerInfo, 0, len(r.peers))
	for _, e := range r.peers {
		peers = append(peers, e.info)
	}
	r.mu.Unlock()
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	_ = c.Send(protocol.Message{Type: protocol.Peers, Peers: peers})
}

func (r *Relay) handleJoin(c *protocol.Conn, m protocol.Message) {
	defer func() { _ = c.Close() }()

	if m.Session == "" || m.From == "" || m.To == "" {
		_ = c.Send(protocol.Message{Type: protocol.Error, Error: "bad relay join"})
		return
	}

	r.mu.Lock()
	if r.peers[m.From] == nil || r.peers[m.To] == nil {
		r.mu.Unlock()
		_ = c.Send(protocol.Message{Type: protocol.Error, Error: "peer not registered"})
		return
	}
	s := r.sessions[m.Session]
	if s == nil {
		s = &pendingSession{
			initiator: m.From,
			target:    m.To,
			done:      make(chan struct{}),
		}
		r.sessions[m.Session] = s
	}

	var other *protocol.Conn
	if m.From == s.initiator {
		if s.a != nil {
			r.mu.Unlock()
			_ = c.Send(protocol.Message{Type: protocol.Error, Error: "duplicate relay join"})
			return
		}
		s.a = c
		other = s.b
	} else {
		if s.b != nil {
			r.mu.Unlock()
			_ = c.Send(protocol.Message{Type: protocol.Error, Error: "duplicate relay join"})
			return
		}
		s.b = c
		other = s.a
	}

	var targetCtrl *protocol.Conn
	if other == nil && m.From == s.initiator {
		if e := r.peers[s.target]; e != nil {
			targetCtrl = e.ctrl
		}
	}
	r.mu.Unlock()

	if targetCtrl != nil {
		offer := protocol.Message{Type: protocol.RelayOffer, Session: m.Session, From: m.From}
		if err := targetCtrl.Send(offer); err != nil {
			r.dropSession(m.Session, s)
			_ = c.Send(protocol.Message{Type: protocol.Error, Error: "target unreachable"})
			return
		}
		r.cfg.Logf("relay session %s offered to %q", m.Session, s.target)
	}

	if other == nil {
		select {
		case <-s.done:
		case <-time.After(r.cfg.JoinTimeout):
			r.dropSession(m.Session, s)
			_ = c.Send(protocol.Message{Type: protocol.Error, Error: "relay session timed out"})
		}
		return
	}

	a, b := s.a, s.b
	if a == nil || b == nil {
		r.dropSession(m.Session, s)
		return
	}
	ready := protocol.Message{Type: protocol.RelayReady, Session: m.Session}
	if err := a.Send(ready); err != nil {
		r.dropSession(m.Session, s)
		return
	}
	if err := b.Send(ready); err != nil {
		r.dropSession(m.Session, s)
		return
	}

	r.cfg.Logf("relay session %s paired", m.Session)
	r.pair(a, b)
	s.finish()
}

func (r *Relay) dropSession(id string, s *pendingSession) {
	r.mu.Lock()
	if r.sessions[id] == s {
		delete(r.sessions, id)
	}
	r.mu.Unlock()
	s.finish()
}

func (r *Relay) pair(a, b *protocol.Conn) {
	ac, abuf := a.Detach()
	bc, bbuf := b.Detach()
	if len(abuf) > 0 {
		_, _ = bc.Write(abuf)
	}
	if len(bbuf) > 0 {
		_, _ = ac.Write(bbuf)
	}
	copyBoth(ac, bc)
}

func copyBoth(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(a, b)
		closeWrite(a)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(b, a)
		closeWrite(b)
	}()
	wg.Wait()
	_ = a.Close()
	_ = b.Close()
}

func closeWrite(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
		return
	}
	_ = c.Close()
}

package peer

import (
	"errors"
	"fmt"
	"net"
	"time"

	"p2pchat/internal/protocol"
)

func (p *Peer) connect(name string) {
	if p.sessionActive() {
		p.fail(errors.New("already connected; disconnect before switching peers"))
		return
	}
	info, ok := p.findPeer(name)
	if !ok {
		p.fail(fmt.Errorf("peer %q is not online", name))
		return
	}
	if name == p.cfg.Name {
		p.fail(errors.New("cannot connect to yourself"))
		return
	}

	p.status("dialing %s directly at %s", name, info.Addr)
	if p.tryDirect(info) {
		return
	}
	p.status("direct connection failed; falling back to relay")
	p.tryRelayed(name)
}

func (p *Peer) tryDirect(info protocol.PeerInfo) bool {
	nc, err := net.DialTimeout("tcp", info.Addr, p.cfg.DialTimeout)
	if err != nil {
		p.cfg.Logf("direct dial %s failed: %v", info.Addr, err)
		return false
	}
	c := protocol.NewConn(nc)
	ok := false
	defer func() {
		if !ok {
			_ = c.Close()
		}
	}()

	if p.cfg.Secret != "" {
		_ = c.SetDeadline(time.Now().Add(protocol.DefaultTimeout))
		if err := protocol.ClientHandshake(c, p.cfg.Secret); err != nil {
			p.cfg.Logf("direct auth failed: %v", err)
			return false
		}
	}
	name, err := exchangeHello(c, p.cfg.Name)
	if err != nil {
		p.cfg.Logf("direct hello failed: %v", err)
		return false
	}
	if err := p.startSession(c, name, Direct); err != nil {
		return false
	}
	ok = true
	return true
}

func (p *Peer) tryRelayed(name string) {
	c, peerName, err := p.openRelayed(protocol.NewChallenge(), name)
	if err != nil {
		p.fail(err)
		return
	}
	if err := p.startSession(c, peerName, Relayed); err != nil {
		_ = c.Close()
	}
}

func (p *Peer) joinRelayed(sid, from string) {
	c, peerName, err := p.openRelayed(sid, from)
	if err != nil {
		p.fail(err)
		return
	}
	if err := p.startSession(c, peerName, Relayed); err != nil {
		_ = c.Close()
	}
}

func (p *Peer) openRelayed(sid, to string) (*protocol.Conn, string, error) {
	c, err := p.dialRelay()
	if err != nil {
		return nil, "", err
	}
	ok := false
	defer func() {
		if !ok {
			_ = c.Close()
		}
	}()

	_ = c.SetDeadline(time.Now().Add(p.cfg.Timeout))
	join := protocol.Message{Type: protocol.RelayJoin, Session: sid, From: p.cfg.Name, To: to}
	if err := c.Send(join); err != nil {
		return nil, "", fmt.Errorf("relay join: %w", err)
	}
	var m protocol.Message
	if err := c.Recv(&m); err != nil {
		return nil, "", fmt.Errorf("relay join: %w", err)
	}
	if m.Type == protocol.Error {
		return nil, "", fmt.Errorf("relay: %s", m.Error)
	}
	if m.Type != protocol.RelayReady {
		return nil, "", fmt.Errorf("relay: unexpected %q", m.Type)
	}
	_ = c.SetDeadline(time.Time{})

	peerName, err := exchangeHello(c, p.cfg.Name)
	if err != nil {
		return nil, "", fmt.Errorf("relayed hello: %w", err)
	}
	ok = true
	return c, peerName, nil
}

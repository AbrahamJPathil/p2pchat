package peer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"p2pchat/internal/protocol"
)

func (s *Session) sendFile(path string) error {
	s.sendFileMu.Lock()
	defer s.sendFileMu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.IsDir() {
		return fmt.Errorf("%s is a directory", path)
	}

	base := filepath.Base(path)
	if !s.send(protocol.DataMessage{Type: protocol.DataFileStart, Name: base, Size: st.Size()}) {
		return errors.New("session closed")
	}
	s.p.emit(Event{Kind: EventTransfer, Peer: s.name, File: base, Size: st.Size()})

	h := sha256.New()
	buf := make([]byte, 32<<10)
	var offset int64
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			_, _ = h.Write(chunk)
			msg := protocol.DataMessage{Type: protocol.DataFileChunk, Offset: offset, Data: chunk}
			if !s.send(msg) {
				return errors.New("session closed")
			}
			offset += int64(n)
			s.progress(Event{Kind: EventTransfer, Peer: s.name, File: base, Size: st.Size(), Progress: offset})
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}

	sum := hex.EncodeToString(h.Sum(nil))
	end := protocol.DataMessage{Type: protocol.DataFileEnd, Name: base, Size: st.Size(), SHA256: sum}
	if !s.send(end) {
		return errors.New("session closed")
	}
	s.p.status("sent %s (%s), sha256 %s", base, humanBytes(offset), sum)
	return nil
}

type incomingFile struct {
	f       *os.File
	h       hash.Hash
	name    string
	size    int64
	written int64
}

func (s *Session) beginIncoming(m protocol.DataMessage) error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.incoming != nil {
		_ = s.incoming.f.Close()
		s.incoming = nil
	}
	base := filepath.Base(strings.TrimSpace(m.Name))
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "received"
	}
	f, err := os.CreateTemp(".", "recv-*-"+base)
	if err != nil {
		return fmt.Errorf("create download: %w", err)
	}
	s.incoming = &incomingFile{f: f, h: sha256.New(), name: base, size: m.Size}
	s.p.emit(Event{Kind: EventTransfer, Peer: s.name, File: base, Size: m.Size})
	return nil
}

func (s *Session) writeIncoming(m protocol.DataMessage) error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.incoming == nil {
		return errors.New("unexpected file chunk")
	}
	if _, err := s.incoming.f.Write(m.Data); err != nil {
		return err
	}
	_, _ = s.incoming.h.Write(m.Data)
	s.incoming.written += int64(len(m.Data))
	s.recv.Add(int64(len(m.Data)))
	s.progress(Event{
		Kind:     EventTransfer,
		Peer:     s.name,
		File:     s.incoming.name,
		Size:     s.incoming.size,
		Progress: s.incoming.written,
	})
	s.stats()
	return nil
}

func (s *Session) endIncoming(m protocol.DataMessage) error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.incoming == nil {
		return errors.New("unexpected file end")
	}
	inc := s.incoming
	s.incoming = nil
	if err := inc.f.Close(); err != nil {
		return err
	}
	got := hex.EncodeToString(inc.h.Sum(nil))
	ok := strings.EqualFold(got, m.SHA256)
	if !ok {
		_ = os.Remove(inc.f.Name())
	}
	s.p.emit(Event{
		Kind:     EventTransfer,
		Peer:     s.name,
		File:     inc.name,
		Size:     inc.size,
		Progress: inc.written,
		Done:     true,
		OK:       ok,
	})
	ack := protocol.DataMessage{Type: protocol.DataFileAck, Name: inc.name, OK: ok}
	if !ok {
		ack.Error = "sha256 mismatch"
	}
	_ = s.c.Send(ack)
	if ok {
		s.p.status("received %s (%s) -> %s", inc.name, humanBytes(inc.written), inc.f.Name())
	} else {
		s.p.status("file %s failed integrity check", inc.name)
	}
	return nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}

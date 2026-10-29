package protocol

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ErrAuth is returned when a challenge response does not match the secret.
var ErrAuth = errors.New("protocol: authentication failed")

// NewChallenge returns a random 128-bit challenge as hex.
func NewChallenge() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should not fail; fall back to a time-based value rather
		// than panicking.
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Answer computes HMAC-SHA256(key = SHA256(secret), message = challenge) and
// returns it as hex. This mirrors bore's auth.rs.
func Answer(secret, challenge string) string {
	key := sha256.Sum256([]byte(secret))
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte(challenge))
	return hex.EncodeToString(mac.Sum(nil))
}

// Validate checks a challenge response in constant time.
func Validate(secret, challenge, answer string) bool {
	expected := Answer(secret, challenge)
	return hmac.Equal([]byte(expected), []byte(answer))
}

// ServerHandshake sends a challenge and verifies the client's answer. The
// caller is responsible for any deadline around the handshake.
func ServerHandshake(c *Conn, secret string) error {
	challenge := NewChallenge()
	if err := c.Send(Message{Type: Challenge, Challenge: challenge}); err != nil {
		return err
	}
	var m Message
	if err := c.Recv(&m); err != nil {
		return err
	}
	if m.Type != Authenticate {
		return ErrAuth
	}
	if !Validate(secret, challenge, m.Answer) {
		return ErrAuth
	}
	return nil
}

// ClientHandshake waits for a challenge and answers it. The caller is
// responsible for any deadline around the handshake.
func ClientHandshake(c *Conn, secret string) error {
	var m Message
	if err := c.Recv(&m); err != nil {
		return err
	}
	if m.Type != Challenge {
		return fmt.Errorf("protocol: expected challenge, got %q", m.Type)
	}
	return c.Send(Message{Type: Authenticate, Answer: Answer(secret, m.Challenge)})
}

package protocol

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestAuthHandshake(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	_ = a.SetDeadline(time.Now().Add(2 * time.Second))
	_ = b.SetDeadline(time.Now().Add(2 * time.Second))

	var wg sync.WaitGroup
	var serverErr, clientErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		serverErr = ServerHandshake(NewConn(a), "shared secret")
	}()
	go func() {
		defer wg.Done()
		clientErr = ClientHandshake(NewConn(b), "shared secret")
	}()
	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client: %v", clientErr)
	}
}

func TestAuthHandshakeWrongSecret(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	_ = a.SetDeadline(time.Now().Add(2 * time.Second))
	_ = b.SetDeadline(time.Now().Add(2 * time.Second))

	var wg sync.WaitGroup
	var serverErr, clientErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		serverErr = ServerHandshake(NewConn(a), "server secret")
	}()
	go func() {
		defer wg.Done()
		clientErr = ClientHandshake(NewConn(b), "client secret")
	}()
	wg.Wait()

	if !errors.Is(serverErr, ErrAuth) {
		t.Fatalf("server err = %v, want ErrAuth", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client err = %v, want nil (it only sends)", clientErr)
	}
}

func TestAnswerAndValidate(t *testing.T) {
	challenge := NewChallenge()
	answer := Answer("hunter2", challenge)

	if !Validate("hunter2", challenge, answer) {
		t.Fatal("valid answer rejected")
	}
	if Validate("hunter3", challenge, answer) {
		t.Fatal("answer accepted with wrong secret")
	}
	if Validate("hunter2", NewChallenge(), answer) {
		t.Fatal("answer accepted for a different challenge")
	}
	if Validate("hunter2", challenge, "not-hex") {
		t.Fatal("garbage answer accepted")
	}
}

func TestChallengeIsRandom(t *testing.T) {
	a, b := NewChallenge(), NewChallenge()
	if a == b {
		t.Fatalf("two challenges are identical: %q", a)
	}
}

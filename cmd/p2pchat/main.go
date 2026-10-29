package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"

	"p2pchat/internal/peer"
	"p2pchat/internal/protocol"
	"p2pchat/internal/relay"
	"p2pchat/internal/tui"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "relay":
		runRelay(os.Args[2:])
	case "peer":
		runPeer(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `p2pchat - peer-to-peer chat and file transfer with a bore-style relay

Usage:
  p2pchat relay [--listen :7835] [--secret S]
  p2pchat peer  [--relay host:7835] [--name NAME] [--secret S]

TUI keys:
  tab          switch between peer list and input
  up/down      select a peer
  enter        connect to selected peer, or send a message
  /send <path> send a file to the connected peer
  ctrl+c       quit
`)
}

func runRelay(args []string) {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)
	addr := fs.String("listen", fmt.Sprintf(":%d", protocol.DefaultRelayPort), "control listen address")
	secret := fs.String("secret", "", "optional shared secret")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r := relay.New(relay.Config{Addr: *addr, Secret: *secret, Logf: log.Printf})
	if err := r.Run(ctx); err != nil {
		log.Fatalf("relay: %v", err)
	}
}

func runPeer(args []string) {
	fs := flag.NewFlagSet("peer", flag.ExitOnError)
	relayAddr := fs.String("relay", fmt.Sprintf("localhost:%d", protocol.DefaultRelayPort), "relay address")
	name := fs.String("name", fmt.Sprintf("peer-%d", os.Getpid()), "name peers see")
	secret := fs.String("secret", "", "optional shared secret")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p := peer.New(peer.Config{RelayAddr: *relayAddr, Name: *name, Secret: *secret})
	if err := p.Start(ctx); err != nil {
		log.Fatalf("peer: %v", err)
	}
	defer p.Close()

	m := tui.New(p)
	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		log.Fatalf("tui: %v", err)
	}
}

# Report / viva notes

## One-line summary

`p2pchat` is a Go peer-to-peer chat and file-transfer tool: a small relay
server helps two named peers discover each other, then the peers try a direct
TCP connection and fall back to a server-relayed connection if NAT or a
firewall gets in the way.

## Problem and motivation

Centralised chat servers carry all traffic and know all content. A peer-to-peer
design lets two hosts talk directly, which is cheaper and demonstrates how
real P2P systems separate **signalling** (finding the other host) from the
**data path** (the actual conversation). The hard part is that most hosts are
behind NAT and cannot accept inbound connections, so a rendezvous/relay
fallback is still needed.

## Architecture

- **Relay (control plane):** tracks `name -> {observed IP, data port, control
  connection}`. Answers discovery requests. Pairs two TCP streams when direct
  dialing fails.
- **Peer (data plane):** registers with the relay, listens on an ephemeral TCP
  port for direct connections, and runs the TUI. The data connection carries
  chat, file chunks, pings, and acks.
- **Why two planes?** The relay only needs to see small JSON control messages
  in the normal case. Once peers connect directly, chat/file bytes never pass
  through the server.

ASCII version is in `README.md`; protocol details are in
`docs/PROTOCOL_NOTES.md`.

## Key design decisions and justifications

1. **TCP, not UDP.**
   Chat and file transfer need reliable, ordered delivery. TCP gives us that
   plus flow control. UDP hole punching is the usual P2P approach, but it is
   unreliable across NAT types and much harder to demo in a short project.

2. **Relay-assisted discovery, not a central chat server.**
   The relay is a rendezvous point. It is on the control path only. This is the
   same split used by many P2P systems (signalling server + data channel).

3. **Direct first, relay fallback.**
   A direct connection has lower latency and does not consume relay bandwidth.
   We dial the address the relay observed. On a LAN this succeeds. Behind
   different NATs it usually fails, and the relay pairs two outbound
   connections, which always works because both peers can reach the relay.

4. **NUL-delimited JSON framing.**
   TCP is a byte stream, not a message stream. A delimiter gives message
   boundaries. JSON is easy to debug and self-describing. The NUL byte cannot
   appear in JSON text, so it is a safe delimiter. A hard maximum frame size
   prevents a malicious or buggy peer from making us allocate unbounded
   memory. This is the same approach bore uses.

5. **One protocol for control and data.**
   Reusing the frame reader/writer keeps the code small. File chunks are
   `[]byte` fields, which Go's `encoding/json` base64-encodes automatically.
   The trade-off is ~33% overhead; a binary frame type is the upgrade path.

6. **Heartbeats and deadlines.**
   TCP can leave a connection open after the other process dies (especially
   through NAT). The relay sends a heartbeat every 5 s and drops a peer after
   15 s without any frame. Peers ping every 2 s, which also refreshes NAT
   mappings and gives an application-level RTT measurement.

7. **HMAC challenge/response, not passwords.**
   The secret is never sent. The server sends a fresh random challenge; the
   client proves knowledge of the secret by computing
   `HMAC-SHA256(SHA256(secret), challenge)`. The server compares in constant
   time. A fresh challenge per connection prevents replaying an old response.
   **Caveat:** this authenticates but does not encrypt. Bore makes the same
   trade-off.

8. **SHA-256 file integrity.**
   The sender hashes the file while streaming. The receiver hashes while
   writing and compares at the end, then sends an ack. A corrupted transfer is
   deleted and reported as failed. This demonstrates end-to-end integrity on
   top of TCP's checksums.

9. **Concurrency model.**
   One goroutine per accepted connection, one per session reader, one per
   session writer, and one for the TUI event loop. Shared state is protected by
   mutexes; counters are atomics; shutdown uses `context` and close channels.
   The TUI never blocks on network I/O: engine methods spawn goroutines and
   report back through a buffered event channel.

## Demo script (5 minutes)

1. Start the relay: `make run-relay`. Point out the control port 7835.
2. Start `alice` and `bob` in two terminals. Point out both names in each peer
   list (relay discovery).
3. Connect alice -> bob. Status line shows **DIRECT** (same machine/LAN) and an
   RTT that updates every 2 s.
4. Send a few chat messages both ways.
5. `/send` a file. Show the progress bar, then the SHA-256 verified status and
   the received `recv-...` file. Run `sha256sum` on both files to prove they
   match.
6. Restart the relay with `--secret demo`, and peers with the same secret.
   Show that a peer with a wrong/missing secret is rejected.
7. Force the fallback: close the direct listener or run the peers on hosts
   where inbound dialing fails. Reconnect; status shows **RELAYED**, chat still
   works. (In the automated test this is done by closing bob's data listener.)
8. Kill one peer. Show the other side reporting a disconnect and returning to
   the peer list.

Useful commands:

```sh
make build
make test
go run ./cmd/p2pchat relay
go run ./cmd/p2pchat peer --name alice
go run ./cmd/p2pchat peer --name bob
sha256sum payload.bin recv-*-payload.bin
```

## Likely viva questions

**Q: Is this really peer-to-peer if there is a server?**
Yes for the data plane when direct dialing succeeds; the server is a
rendezvous/signalling service and a fallback relay. Many P2P systems use a
server to bootstrap. The chat/file bytes do not go through the relay in DIRECT
mode.

**Q: Why not use UDP and hole punching?**
UDP hole punching requires simultaneous sends, NAT-type detection, and
keepalives, and still fails for symmetric NATs. TCP direct dial plus a relay
fallback is simpler and reliable enough to demo. The relay is the guaranteed
path.

**Q: How does the relay know a peer's public address?**
It uses the source IP of the peer's control TCP connection and the data port
the peer advertised in `Hello`. It does not trust the peer's claimed IP, only
its port.

**Q: What happens if two peers register the same name?**
The newest registration replaces the old control connection. Names are labels,
not authenticated identities. The secret authenticates access to the relay, not
individual names. A production system would bind names to keys.

**Q: How is a message delimited?**
A JSON object followed by a NUL byte. The reader uses `ReadSlice(0)` with a
buffer of `maxFrame+1`, so a frame larger than 64 KiB is rejected with
`ErrFrameTooLarge` instead of growing memory.

**Q: Why is there a maximum frame size?**
Without it, a hostile peer could send an endless frame and exhaust memory. The
limit also bounds the base64-encoded file chunk size.

**Q: How does the relay pair two connections?**
The initiator sends `RelayJoin` on a new data connection. The relay notifies the
target over its control connection with `RelayOffer`. The target opens its own
data connection and sends `RelayJoin`. When both are present, the relay sends
`RelayReady` to each and runs two `io.Copy` goroutines between the raw sockets.
This mirrors bore's `Connection(id)` / `Accept(id)` pairing.

**Q: How do you avoid losing bytes when switching from framed control to raw
proxying?**
`Conn.Detach()` returns the underlying socket plus anything already buffered by
the frame reader. The relay writes those buffered bytes to the other side
before starting `io.Copy`. Bore does the same with `FramedParts.read_buf`.

**Q: How does half-close work?**
Two independent `io.Copy` directions plus `CloseWrite` on each side. A FIN in
one direction does not tear down the other direction. The Rust e2e test checks
this; our proxy follows the same pattern.

**Q: How is authentication replay-resistant?**
The server generates a new random 128-bit challenge for every connection. The
response is an HMAC over that challenge, so a recorded response is useless on a
different connection.

**Q: Does the secret encrypt traffic?**
No. It only proves knowledge of the secret. Anyone who can sniff the TCP stream
can read chat and files. Adding TLS or Noise would be the next step.

**Q: How is file integrity checked?**
SHA-256 over the whole file, computed while streaming. The receiver compares
the digest to the `file_end` message, deletes the file on mismatch, and sends a
`file_ack` with `ok=false`. The sender displays the failure.

**Q: What are the failure modes you handled?**
Relay unreachable, port in use, handshake timeout, wrong secret, peer
disconnect, relay connection lost, oversized frame, unknown peer, busy session,
file open/write errors, SHA-256 mismatch, and TUI quit. Errors are surfaced in
the status line; the process does not panic.

**Q: What would you improve with more time?**
TLS/Noise encryption, UDP hole punching with NAT-type detection, resumable
transfers, multiple concurrent sessions, a persistent peer identity (public
keys), binary file frames to remove base64 overhead, and a configurable relay
port range.

## Limitations to state honestly

- Direct connections across two different NATs will usually show `RELAYED`;
  true hole punching is not implemented.
- One active conversation per peer.
- Base64 chunk overhead.
- No encryption, no resumable transfers, no offline messages.
- Relay state is in memory; restarting the relay clears the peer list.

## Credits

Protocol design and framing adapted from
[bore](https://github.com/ekzhang/bore) by Eric Zhang, MIT licensed. The
original `LICENSE` is preserved.

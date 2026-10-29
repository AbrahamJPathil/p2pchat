# p2pchat

A small peer-to-peer chat and file-transfer application in Go, with a
terminal UI and a bore-style rendezvous relay. Two peers find each other
through the relay, try a **DIRECT** TCP connection first, and fall back to a
**RELAYED** connection through the server when direct dialing is impossible.

Built for an S5 Computer Networks micro project. The protocol ideas are
adapted from [bore](https://github.com/ekzhang/bore) (MIT); see
`docs/PROTOCOL_NOTES.md` for the source study.

## Features

- Named peers register with a relay and appear in a live peer list.
- Direct TCP connection on a LAN; relay fallback through the server.
- Text chat.
- File transfer with a progress bar and SHA-256 integrity check.
- Optional shared-secret authentication using an HMAC challenge/response.
- TUI shows connection type (`DIRECT` / `RELAYED`), RTT, and payload bytes
  sent/received.
- Heartbeats and read deadlines remove dead peers.
- Graceful handling of disconnects, timeouts, busy sessions, and bad frames.

## Architecture

```
                    control: null-delimited JSON frames
   +-----------+  <-------------------------------------->  +-----------+
   |   alice   |                 relay                      |    bob    |
   |   peer    |  <-------------------------------------->  |   peer    |
   +-----------+        discovery, heartbeats, auth         +-----------+
         |                                                         |
         |  DIRECT: dial bob's advertised data port (preferred)    |
         +=========================================================+
         |                                                         |
         |  RELAYED: two data TCP streams paired by the relay      |
         +----------------------> relay <--------------------------+
                                    |
                          io.Copy both directions
```

The relay is a **rendezvous server**: it does not store messages. It keeps
`name -> {public address, control connection}` in memory. Each peer also
listens on an ephemeral TCP port for direct connections. The relay observes
the peer's source IP and advertises `observedIP:dataPort`, so the same
registration works on localhost, a LAN, or behind NAT.

### Connection sequence

```
alice                     relay                      bob
  |  Hello{name,dataPort}   |                         |
  |------------------------>|  Welcome{publicAddr}    |
  |<------------------------|                         |
  |  List                   |                         |
  |------------------------>|  Peers[{bob,...}]       |
  |<------------------------|                         |
  |                         |                         |
  |  direct TCP dial to bob's publicAddr              |
  |==================================================>|  (DIRECT)
  |                         |                         |
  |  (if the dial fails)    |                         |
  |  RelayJoin{session}     |  RelayOffer{session}    |
  |------------------------>|------------------------>|
  |                         |  RelayJoin{session}     |
  |<------------------------|<------------------------|
  |  RelayReady             |  RelayReady             |
  |<------------------------|------------------------>|
  |============ raw data, relay copies bytes =========|  (RELAYED)
```

Once a session exists, peers exchange `DataMessage` frames: `hello`, `chat`,
`ping`/`pong` (RTT), `file_start`/`file_chunk`/`file_end`/`file_ack`, and
`bye`.

## Quick start

Requirements: Go 1.24.2+ (tested with Go 1.26), a terminal.

```sh
cd p2pchat
make build            # produces bin/p2pchat
make test             # unit + integration tests
```

Run the demo in three terminals:

```sh
# Terminal 1 - relay
make run-relay
# or: go run ./cmd/p2pchat relay

# Terminal 2 - first peer
make run-peer NAME=alice
# or: go run ./cmd/p2pchat peer --name alice

# Terminal 3 - second peer
make run-peer NAME=bob
# or: go run ./cmd/p2pchat peer --name bob
```

In each peer TUI:

1. Wait for the other name to appear under **PEERS**.
2. Press `Tab` to focus the peer list, use `Up`/`Down`, press `Enter` to
   connect.
3. Press `Tab` to focus the input, type a message, press `Enter`.
4. Send a file with `/send /path/to/file`.
5. Press `Ctrl+C` to quit.

The status line shows the mode, RTT, and byte counters. A received file is
written to the current directory as `recv-<random>-<original-name>`; the status
line shows the exact path and the SHA-256 result.

### With a shared secret

```sh
go run ./cmd/p2pchat relay --secret demo-secret
go run ./cmd/p2pchat peer --name alice --secret demo-secret
go run ./cmd/p2pchat peer --name bob   --secret demo-secret
```

If one side has the wrong secret, registration/handshake fails with an error.

### Across two machines

Run the relay on a machine both peers can reach:

```sh
# machine A
go run ./cmd/p2pchat relay --listen :7835

# machines B and C
go run ./cmd/p2pchat peer --relay A_IP:7835 --name alice
go run ./cmd/p2pchat peer --relay A_IP:7835 --name bob
```

Allow inbound TCP 7835 on the relay host. If the two peers are on the same
LAN, direct dialing usually succeeds (`DIRECT`). If they are behind different
NATs, expect `RELAYED`; that is the intended fallback.

## Project layout

```
p2pchat/
├── cmd/p2pchat/main.go       CLI: relay and peer subcommands (stdlib flag)
├── internal/protocol/        frames, message types, HMAC auth
├── internal/relay/           registry, heartbeats, session pairing, byte proxy
├── internal/peer/            registration, discovery, direct/relay connect,
│                             chat, file transfer, RTT/byte counters
├── internal/tui/             Bubble Tea + Lipgloss + Bubbles UI
├── docs/PROTOCOL_NOTES.md    study of the Rust bore implementation
├── docs/REPORT_NOTES.md      talking points for the report/viva
├── Makefile
└── LICENSE                   MIT (from bore)
```

## Protocol in one page

- **Framing:** JSON followed by a NUL byte (`\0`), max 64 KiB per frame.
  Oversized frames are rejected. One `Conn` type serves both control and data
  traffic.
- **Control messages:** `hello`/`welcome`, `list`/`peers`, `heartbeat`/`ping`,
  `relay_join`/`relay_offer`/`relay_ready`, `challenge`/`authenticate`,
  `error`.
- **Data messages:** `hello`, `chat`, `ping`/`pong`, `file_start`,
  `file_chunk`, `file_end`, `file_ack`, `bye`. File bytes are carried in a
  `[]byte` JSON field, which Go automatically base64-encodes.
- **Auth (optional):** server sends `challenge` (16 random bytes). Client
  replies with `hex(HMAC-SHA256(SHA256(secret), challenge))`. The server
  compares in constant time. New challenge per connection.
- **Liveness:** relay sends `heartbeat` every 5 s; peers send `ping` every 2 s
  and refresh the peer list. The relay drops a control connection after 15 s of
  silence. Data sessions use a 10 s read deadline refreshed by pings.
- **Relay fallback:** the initiator opens a second TCP connection to the relay
  and sends `relay_join`; the relay notifies the target over its control
  connection; the target joins; the relay sends `relay_ready` to both and
  copies bytes with two `io.Copy` goroutines.

## CN concepts demonstrated

- **TCP sockets and streams:** listeners, dialing, `io.Copy`, `CloseWrite`,
  half-close, ephemeral ports.
- **Client-server vs peer-to-peer:** the relay is a control-plane server; the
  data plane is peer-to-peer when possible and server-mediated otherwise.
- **NAT traversal / rendezvous:** peers register an observed public address;
  direct dialing works on a LAN, and the relay is the fallback when NAT or a
  firewall blocks inbound connections.
- **Application-layer framing:** TCP is a byte stream; NUL-delimited JSON
  frames give message boundaries and a maximum frame size.
- **Heartbeats and timeouts:** detecting dead peers without relying on TCP
  FIN/RST, and keeping NAT mappings alive.
- **Authentication:** HMAC-SHA256 challenge/response with a per-connection
  nonce, constant-time comparison.
- **Integrity:** SHA-256 over the file, checked by the receiver and acknowledged
  to the sender.
- **Concurrency:** goroutine per connection/session, channels for TUI events,
  `context` for shutdown, mutexes for shared state, atomic counters.
- **Protocol design:** version-free minimal envelopes, explicit error messages,
  bounded frames, and graceful failure paths.

## Tests

```sh
make test
# or: go test ./...
```

Coverage includes:

- framing round trips, multiple frames, oversized-frame rejection, binary file
  chunks, and read deadlines (`internal/protocol`);
- HMAC handshake success/failure and challenge randomness;
- relay registration, offer/join/ready pairing, bidirectional bytes, and
  unknown-peer errors (`internal/relay`);
- two real peers over a real relay: direct connection, chat, file transfer with
  SHA-256 verification, and forced relay fallback (`internal/peer`).

## Assumptions and limitations

- **No encryption.** The secret authenticates peers; it does not encrypt chat
  or files. Do not use this for sensitive data.
- **Direct NAT traversal is best-effort.** We try a direct dial to the
  relay-observed address. True hole punching and symmetric NATs are out of
  scope; the relay is the guaranteed path.
- **One active conversation per peer.** Disconnect (restart the peer or wait
  for the session to end) before switching to another peer.
- File chunks are base64 inside JSON for simplicity, which costs about 33%
  overhead. A binary data frame is the upgrade path.
- Transfers are not resumable. A disconnect aborts the transfer.
- Peer names are not unique identities; the relay replaces a duplicate name
  with the newest registration.
- Byte counters count chat/file payload, not TCP or JSON framing overhead.
- The relay keeps state in memory only.

## Credit

Protocol design and framing adapted from
[bore](https://github.com/ekzhang/bore) by Eric Zhang, MIT licensed. The
original license text is kept in `p2pchat/LICENSE`.

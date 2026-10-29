# Protocol notes: what we borrowed from bore

Reference studied: the Rust sources in the repository root (`src/shared.rs`,
`src/auth.rs`, `src/server.rs`, `src/client.rs`). Bore is MIT licensed. The Go
implementation in `p2pchat/` reuses the *ideas*, not the code.

## 1. Transport and framing

Bore's control connection is plain TCP on port **7835** (`CONTROL_PORT` in
`src/shared.rs:15`). Every message is JSON followed by a NUL byte (`\0`). The
reader is a `tokio_util` `AnyDelimiterCodec` with NUL as both start and end
delimiter, a **256-byte maximum frame**, and a **3-second network timeout**
(`src/shared.rs:17-21`, `:56-63`). A frame that exceeds the limit is rejected
and the connection is dropped.

`Delimited::recv_timeout` is used for the initial handshake so a silent peer
cannot hold a connection forever (`src/shared.rs:78-86`). After the handshake
the connection runs without a per-message timeout; liveness comes from
heartbeats instead.

**Go mapping:** `internal/protocol/message.go`. `Conn` wraps `net.Conn` with a
`bufio.Reader` sized to the maximum frame, uses `ReadSlice(0)` to reject
oversized frames without unbounded allocation, and serializes writes with a
mutex. Frames are JSON + `\0`. The Go limit is 64 KiB so 32 KiB file chunks fit
inside a frame.

## 2. Message types

Bore uses two Rust enums serialized with serde's default (externally tagged)
representation.

Client -> server (`src/shared.rs:24-35`):

| Variant | Payload | Meaning |
|---|---|---|
| `Authenticate(String)` | HMAC hex tag | response to an auth challenge |
| `Hello(u16)` | requested public port, 0 = any | register a tunnel |
| `Accept(Uuid)` | connection id | claim a pending incoming TCP stream |

Server -> client (`src/shared.rs:37-53`):

| Variant | Payload | Meaning |
|---|---|---|
| `Challenge(Uuid)` | random challenge | first message when a secret is configured |
| `Hello(u16)` | allocated public port | registration accepted |
| `Heartbeat` | none | liveness probe, sent repeatedly |
| `Connection(Uuid)` | connection id | a new public TCP connection is waiting |
| `Error(String)` | text | fatal error, server closes |

On the wire a unit variant is a JSON string (`"Heartbeat"`), a newtype variant
is a one-key object (`{"Hello":8080}`), e.g. `{"Hello":8080}\0`.

**Go mapping:** one `Message` envelope with a `type` field plus optional
fields (`internal/protocol/message.go`). This keeps one decoder for control and
data traffic. Go adds peer discovery and relay-session messages:
`Hello`/`Welcome`, `List`/`Peers`, `Heartbeat`/`Ping`, `RelayJoin`,
`RelayOffer`, `RelayReady`, `Challenge`/`Authenticate`, `Error`. Data-plane
frames use a second envelope `DataMessage` (`hello`, `chat`, `ping`, `pong`,
`file_start`, `file_chunk`, `file_end`, `file_ack`, `bye`).

## 3. Control handshake and connection lifecycle

Server side (`src/server.rs:118-186`):

1. Wrap the accepted TCP stream in the delimited JSON codec.
2. If a secret is configured, run the server auth handshake
   (`Challenge` -> expect `Authenticate`). On failure send `Error` and stop.
3. Read the first message with a timeout. It must be `Hello(port)`.
4. Allocate a public listener (see below). Reply `Hello(actual_port)`.
5. Loop: send `Heartbeat`; wait up to 500 ms for a new connection on the public
   listener. When one arrives, generate a UUID, store the raw stream in a
   concurrent map (`DashMap<Uuid, TcpStream>`), schedule its removal after 10 s,
   and send `Connection(id)` on the control connection.
6. When the client later sends `Accept(id)` on a *new* control connection, the
   server removes the stored stream and starts proxying.

Client side (`src/client.rs:43-118`):

1. Connect to the server control port with a timeout.
2. If a secret is configured, answer the server's challenge.
3. Send `Hello(requested_port)`, expect `Hello(allocated_port)` or `Error`.
4. Loop on the control connection: ignore `Heartbeat`, and for each
   `Connection(id)` spawn a task that opens a new control connection, repeats
   auth, sends `Accept(id)`, connects to the local service, and proxies bytes.

The extra connection per forwarded stream is what lets the server pair the
incoming public socket with a fresh client socket; the original control
connection stays available for more `Connection` messages.

**Go mapping:** the relay keeps a registry instead of exposing one public port
per client. `Hello{name, dataAddr}` registers a peer; `Welcome` returns the
observed public address. `List`/`Peers` implement discovery. When direct dial
fails, `RelayJoin` creates a pending session, `RelayOffer` wakes the target over
its control connection, and the second `RelayJoin` makes the relay send
`RelayReady` to both sides and start copying. Heartbeats and read deadlines
replace bore's 500 ms accept poll: the relay sends `Heartbeat` every 5 s, peers
send `Ping` every 2 s, and the relay drops a peer after 15 s of silence.

## 4. Public port allocation

`Server::create_listener` (`src/server.rs:81-116`):

- A requested non-zero port must be inside `[min_port, max_port]`, otherwise
  the client gets "client port number not in allowed range".
- Port 0 means "any": bore tries up to **150 random ports** in the range. The
  comment explains this gives ~99.999% success at using 85% of the range.
- Bind errors are translated: address in use, permission denied, generic bind
  failure.

**Go mapping:** not needed for a peer-to-peer app. Each peer binds an ephemeral
data listener (`:0`) and advertises only its port; the relay replaces the host
with the IP it observes, so the same registration works on a LAN and behind
NAT. The relay itself listens on one control port (default 7835).

## 5. Optional shared-secret authentication

`src/auth.rs`:

- The HMAC key is `SHA256(secret)`, so any secret length is accepted.
- `answer(challenge)` = `hex(HMAC-SHA256(key, challenge_uuid_bytes))`.
- `validate` decodes the hex tag and uses the HMAC crate's constant-time
  `verify_slice`.
- `server_handshake`: generate a random UUID challenge, send `Challenge`,
  read `Authenticate`, validate; anything else is an error.
- `client_handshake`: expect `Challenge`, compute the tag, send
  `Authenticate`.

This is a challenge/response that proves knowledge of the secret. It does
**not** encrypt traffic; bore is a tunnel, not a VPN. A new random challenge per
connection prevents replaying an old tag on a new connection.

**Go mapping:** `internal/protocol/auth.go` uses the same construction:
`key = SHA256(secret)`, `HMAC-SHA256(key, challenge)`, hex output, constant-time
`hmac.Equal`. The challenge is 16 random bytes from `crypto/rand`. The same
handshake protects relay control connections, relayed data connections, and
direct peer connections.

## 6. Bidirectional byte proxying

When the server receives `Accept(id)` it removes the stored stream and calls
`tokio::io::copy_bidirectional(&mut control_stream, &mut public_stream)`
(`src/server.rs:171-183`). Before copying it writes any bytes already buffered
by the JSON codec into the public stream (`parts.read_buf`), so no bytes are
lost when a stream switches from framed messages to raw proxying. The client
does the same with its local service (`src/client.rs:105-117`).

`copy_bidirectional` keeps both directions alive independently, which preserves
TCP half-close semantics: one side can send FIN while the other direction is
still usable. The e2e test `half_closed_tcp_stream` checks this
(`tests/e2e_test.rs:130-164`).

**Go mapping:** `internal/relay/relay.go` uses `protocol.Conn.Detach()` to hand
over the raw socket plus any buffered bytes, then runs two `io.Copy` goroutines
with `CloseWrite` on each side. The peer data session is also full duplex: a
writer goroutine serializes chat/file frames and periodic pings, while a reader
goroutine handles incoming frames. File integrity is checked with SHA-256 at the
end of the transfer.

## 7. Deliberate differences from bore

| Bore | p2pchat |
|---|---|
| Exposes a local port to the public internet | Connects two named peers |
| Server allocates a public port per client | Relay keeps a name -> address registry |
| 256-byte JSON control frames | 64 KiB JSON frames, also used for data |
| Raw bytes after `Accept` | Peer `DataMessage` frames for chat/files |
| 3 s initial timeout, 500 ms accept poll | 5 s handshake timeout, heartbeat + read deadlines |
| Multiple tunnels per client | One active peer conversation at a time |
| No encryption | No encryption (same caveat) |

## Credit

Protocol design and framing are adapted from
[bore](https://github.com/ekzhang/bore) by Eric Zhang, MIT licensed. The
original `LICENSE` is kept at `p2pchat/LICENSE`.

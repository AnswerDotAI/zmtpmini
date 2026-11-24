# zmtpmini

Minimal async ZMTP 3.1 for Rust: connect-side DEALER and SUB sockets, and ROUTER, PUB, XPUB and REP peers over connections the caller accepts (`Peer`), over TCP or any tokio byte stream.

zmtpmini implements the corner of ZeroMQ that a Jupyter-style client needs. It connects to a peer's ROUTER or XPUB socket, completes the ZMTP 3.1 NULL-security handshake (carrying an `Identity` for DEALER), and exchanges multipart messages. The crate does not use libzmq. It implements the protocol directly on tokio streams. It cross-compiles anywhere Rust does, and depends only on `tokio` and `bytes`.

## Scope

| Included | Deliberately absent |
| --- | --- |
| DEALER and SUB on the connect side; ROUTER, PUB, XPUB and REP over a connection the caller accepted (`Peer`) | binding and listening, and all other socket types |
| ZMTP 3.1, NULL security | ZMTP 1.0/2.0/3.0, CURVE, PLAIN |
| PING answered with PONG; unknown commands skipped | sending PINGs |
| TCP (with `TCP_NODELAY`) and generic `AsyncRead + AsyncWrite` streams | automatic reconnection |
| a configurable frame-size cap (default 64 MiB) | unbounded internal queues |

A dropped connection returns `Error::Closed`, while I/O failures and protocol violations retain their own error variants. The crate never redials or buffers on its own.

## Use

```rust
use zmtpmini::{Dealer, Sub};

let mut shell = Dealer::connect("127.0.0.1:5555", Some(b"my-session")).await?;
shell.send([&b"<IDS|MSG>"[..], b"", header, b"{}", b"{}", b"{}"]).await?;
let reply = shell.recv().await?;

let mut iopub = Sub::connect("127.0.0.1:5556").await?;
iopub.subscribe(b"").await?;
let msg = iopub.recv().await?;
```

`send` accepts multipart messages and `recv` returns their frames as `Vec<Bytes>`. Partial reads and staged writes survive cancellation. If either returns an error, discard the socket.

Use `let (mut rx, tx) = dealer.split();` to receive and send from separate tasks. Call `rx.recv()` and `tx.send(parts)` as on the unsplit socket. A blocked send doesn't stop ordinary reads. If the sender is cancelled with bytes left to write, the reader finishes the message while waiting for input. The reader also answers ZMTP PINGs between complete messages. There is no background task or outgoing queue.

For a receive-only handle to a SUB socket, set its subscriptions and call `sub.into_reader()`. This returns the same `ClientReader` as `dealer.split()`.

`with_max_frame` sets a socket's cap on the body size of each frame it receives, 64 MiB until set. `ZmtpStream::handshake` is the lower-level entry point for non-TCP streams. `peer_meta()` exposes the peer's handshake metadata, such as its announced socket type.

## Development

```bash
cargo test
```

The integration tests run against a real kernel and pyzmq peers. Install them first with `pip install ipymini pyzmq`. Set `ZMTPMINI_TEST_PYTHON` to choose which Python runs them.

## Release

```bash
cargo test
ship-release
```

`ship-release` tags the Cargo version and pushes; CI publishes to crates.io via trusted publishing and creates the GitHub release, then fastship bumps `Cargo.toml`.

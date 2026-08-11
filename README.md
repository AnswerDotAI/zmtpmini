# zmtpmini

Minimal async ZMTP 3.1 client for Rust: connect-side DEALER and SUB sockets over TCP, or over any tokio byte stream.

zmtpmini implements the corner of ZeroMQ that a Jupyter-style client needs. It connects to a peer's ROUTER or XPUB socket, completes the ZMTP 3.1 NULL-security handshake (carrying an `Identity` for DEALER), and exchanges multipart messages. The crate does not use libzmq. It implements the protocol directly on tokio streams. It cross-compiles anywhere Rust does, and depends only on `tokio` and `bytes`.

## Scope

| Included | Deliberately absent |
| --- | --- |
| DEALER and SUB, connect side | bind, and all other socket types |
| ZMTP 3.1, NULL security | ZMTP 1.0/2.0/3.0, CURVE, PLAIN |
| PING answered with PONG; unknown commands skipped | sending PINGs |
| TCP (with `TCP_NODELAY`) and generic `AsyncRead + AsyncWrite` streams | automatic reconnection |
| a configurable frame-size cap (default 64 MiB) | unbounded internal queues |

A dropped connection or a protocol violation returns an error. The crate never redials or buffers on its own.

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

`send` and `recv` take and return multipart messages as sequences of frames (`Vec<Bytes>` on the way out). Both are cancellation-safe. All progress lives in the socket, so a `recv` dropped by a `select!` or timeout never loses a message, and a cancelled `send` resumes its flush on the next call. Sockets take `&mut self`, so the borrow checker enforces what libzmq documents as "sockets are not thread-safe". If a call returns `Err`, the connection is unusable. Discard it.

`ZmtpStream::handshake` is the lower-level entry point for non-TCP streams or a non-default frame cap. `peer_meta()` exposes the peer's handshake metadata, such as its announced socket type.

## Development

```bash
cargo test
```

The integration tests run against a real kernel. Install it first with `pip install ipymini`. Set `ZMTPMINI_TEST_PYTHON` to choose which Python runs it.

## Release

```bash
cargo test
ship-release
```

`ship-release` tags the Cargo version and pushes; CI publishes to crates.io via trusted publishing and creates the GitHub release, then fastship bumps `Cargo.toml`.

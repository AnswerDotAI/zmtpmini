//! Sync ZMTP 3.1 codec: greetings, frames, and commands over byte buffers.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Errors from codec and socket operations. Any error leaves the connection unusable: discard it.
#[derive(Debug)]
pub enum Error {
    /// Underlying I/O failure.
    Io(std::io::Error),
    /// The peer closed an established connection.
    Closed,
    /// The peer violated ZMTP or spoke an unsupported variant.
    Protocol(String),
}

impl From<std::io::Error> for Error { fn from(e: std::io::Error) -> Self { Error::Io(e) } }

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"), Error::Closed => write!(f, "connection closed by peer"),
            Error::Protocol(m) => write!(f, "protocol error: {m}")
        }
    }
}

impl std::error::Error for Error {}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn perr<T>(msg: impl Into<String>) -> Result<T> { Err(Error::Protocol(msg.into())) }

/// Byte length of a ZMTP greeting.
pub const GREETING_LEN: usize = 64;

/// Encode our greeting: ZMTP 3.1, NULL mechanism, as-client.
pub fn encode_greeting(as_server: bool) -> [u8; GREETING_LEN] {
    let mut g = [0u8; GREETING_LEN];
    g[0] = 0xFF;
    g[9] = 0x7F;
    g[10] = 3;
    g[11] = 1;
    g[12..16].copy_from_slice(b"NULL");
    g[32] = as_server.into();
    g
}

/// Validate a peer greeting, requiring ZMTP >= 3.1 and the NULL mechanism.
pub fn check_greeting(g: &[u8; GREETING_LEN]) -> Result<()> {
    if g[0] != 0xFF || g[9] & 1 != 1 { return perr("not a ZMTP peer (bad signature)"); }
    if g[10] != 3 || g[11] < 1 { return perr(format!("peer speaks ZMTP {}.{}; zmtpmini requires 3.1", g[10], g[11])); }
    let mech = &g[12..32];
    if &mech[..4] != b"NULL" || mech[4..].iter().any(|&b| b != 0) {
        return perr(format!(
            "peer requires security mechanism {:?}; only NULL is supported", String::from_utf8_lossy(&mech[..mech.iter().position(|&b| b == 0).unwrap_or(20)])
        ));
    }
    Ok(())
}

const MORE: u8 = 1;
const LONG: u8 = 2;
const COMMAND: u8 = 4;

/// One decoded ZMTP frame.
#[derive(Debug)]
pub struct RawFrame {
    /// Command frame (vs message frame).
    pub command: bool,
    /// More message frames follow.
    pub more: bool,
    /// Frame body.
    pub body: Bytes,
}

/// Decode one frame from `buf`, returning `None` when more bytes are needed.
pub fn decode_frame(buf: &mut BytesMut, max_frame: usize) -> Result<Option<RawFrame>> {
    if buf.len() < 2 { return Ok(None); }
    let flags = buf[0];
    if flags & !(MORE | LONG | COMMAND) != 0 { return perr("reserved frame flag bits set"); }
    let (hdr, len) = if flags & LONG != 0 {
        if buf.len() < 9 { return Ok(None); }
        (9, u64::from_be_bytes(buf[1..9].try_into().unwrap()) as usize)
    } else { (2, buf[1] as usize) };
    if len > max_frame { return perr(format!("frame of {len} bytes exceeds cap of {max_frame}")); }
    if buf.len() < hdr + len { return Ok(None); }
    buf.advance(hdr);
    let body = buf.split_to(len).freeze();
    Ok(Some(RawFrame { command: flags & COMMAND != 0, more: flags & MORE != 0, body }))
}

/// Append one encoded frame to `out`.
pub fn encode_frame(body: &[u8], more: bool, command: bool, out: &mut BytesMut) {
    let mut flags = if more { MORE } else { 0 } | if command { COMMAND } else { 0 };
    if body.len() > 255 {
        flags |= LONG;
        out.put_u8(flags);
        out.put_u64(body.len() as u64);
    } else {
        out.put_u8(flags);
        out.put_u8(body.len() as u8);
    }
    out.put_slice(body);
}

/// A parsed ZMTP command frame.
#[derive(Debug)]
pub enum Command {
    /// Handshake metadata: (name, value) properties.
    Ready(Vec<(String, Bytes)>),
    /// Fatal error from the peer.
    Error(String),
    /// Heartbeat request; answer with PONG carrying `context`.
    Ping {
        /// Time to live, 1/100ths of a second (informational).
        #[allow(dead_code)]
        ttl: u16,
        /// Opaque context echoed in the PONG.
        context: Bytes,
    },
    /// Subscribe to a topic prefix.
    Subscribe(Bytes),
    /// Cancel a topic-prefix subscription.
    Cancel(Bytes),
    /// Any other command; ignored per spec.
    Other,
}

fn short_str(buf: &mut Bytes, what: &str) -> Result<String> {
    if buf.is_empty() { return perr(format!("truncated {what}")); }
    let n = buf.get_u8() as usize;
    if buf.len() < n { return perr(format!("truncated {what}")); }
    Ok(String::from_utf8_lossy(&buf.split_to(n)).into_owned())
}

/// Parse a command frame body.
pub fn parse_command(body: &Bytes) -> Result<Command> {
    let mut b = body.clone();
    let name = short_str(&mut b, "command name")?;
    match name.as_str() {
        "READY" => {
            let mut meta = vec![];
            while !b.is_empty() {
                let k = short_str(&mut b, "metadata name")?;
                if b.len() < 4 { return perr("truncated metadata value"); }
                let n = b.get_u32() as usize;
                if b.len() < n { return perr("truncated metadata value"); }
                meta.push((k, b.split_to(n)));
            }
            Ok(Command::Ready(meta))
        }
        "ERROR" => Ok(Command::Error(short_str(&mut b, "error reason")?)),
        "PING" => { if b.len() < 2 { return perr("truncated PING"); } Ok(Command::Ping { ttl: b.get_u16(), context: b }) }
        "SUBSCRIBE" => Ok(Command::Subscribe(b)),
        "CANCEL" => Ok(Command::Cancel(b)),
        _ => Ok(Command::Other),
    }
}

fn put_property(out: &mut BytesMut, name: &str, value: &[u8]) {
    out.put_u8(name.len() as u8);
    out.put_slice(name.as_bytes());
    out.put_u32(value.len() as u32);
    out.put_slice(value);
}

/// Encode a READY command body with `Socket-Type` and optional `Identity` metadata.
pub fn ready_command(socket_type: &str, identity: Option<&[u8]>) -> Bytes {
    let mut out = BytesMut::new();
    out.put_slice(b"\x05READY");
    put_property(&mut out, "Socket-Type", socket_type.as_bytes());
    if let Some(id) = identity { put_property(&mut out, "Identity", id) }
    out.freeze()
}

/// Encode a SUBSCRIBE command body (empty `topic` subscribes to everything).
pub fn subscribe_command(topic: &[u8]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_slice(b"\x09SUBSCRIBE");
    out.put_slice(topic);
    out.freeze()
}

/// Encode a CANCEL command body.
pub fn cancel_command(topic: &[u8]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_slice(b"\x06CANCEL");
    out.put_slice(topic);
    out.freeze()
}

/// Encode a PONG command body echoing `context`.
pub fn pong_command(context: &[u8]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_slice(b"\x04PONG");
    out.put_slice(context);
    out.freeze()
}

/// Is `theirs` a valid peer socket type for `ours`?
pub fn compatible(ours: &str, theirs: &str) -> bool {
    match ours {
        "DEALER" => matches!(theirs, "ROUTER" | "DEALER" | "REP"), "SUB" => matches!(theirs, "PUB" | "XPUB"), "REQ" => matches!(theirs, "ROUTER" | "REP"),
        "ROUTER" => matches!(theirs, "DEALER" | "REQ" | "ROUTER"), "PUB" | "XPUB" => theirs == "SUB", "REP" => matches!(theirs, "REQ" | "DEALER"), _ => false
    }
}

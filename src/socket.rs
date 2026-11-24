//! Async ZMTP sockets: handshake, multipart send/receive, `Dealer` and `Sub`.

use bytes::{Bytes, BytesMut};
use std::{mem, sync::Arc};
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, MutexGuard, Notify};

use crate::wire::*;

/// Default cap on a single frame's body size (64 MiB).
pub const DEFAULT_MAX_FRAME: usize = 64 * 1024 * 1024;

/// Low-level ZMTP 3.1 connection over any async byte stream.
///
/// `recv_multipart` and `send_multipart` are cancellation-safe. All progress
/// (read buffer, partially received message, unflushed writes) lives in the
/// socket, so a dropped future loses nothing and the next call resumes. If a
/// call returns `Err`, the connection is unusable. Discard it.
pub struct ZmtpStream<S> { rx: ClientReader<S>, peer_meta: Vec<(String, Bytes)> }

impl<S: AsyncRead + AsyncWrite + Unpin> ZmtpStream<S> {
    /// Perform the ZMTP 3.1 NULL handshake as `socket_type`, with optional Identity metadata.
    pub async fn handshake(s: S, socket_type: &str, identity: Option<&[u8]>, max_frame: usize) -> Result<Self> {
        let Peer { reader, writer, peer_meta } = Peer::handshake(s, false, socket_type, identity, max_frame).await?;
        let writer = DealerWriter(Arc::new(Outbound { writer: Mutex::new(writer), started: Notify::new() }));
        let rx = ClientReader { reader, writer, ping: None };
        Ok(Self { rx, peer_meta })
    }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { &self.peer_meta }

    /// Send one multipart message.
    pub async fn send_multipart(&mut self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> { self.rx.writer.send(parts).await }

    /// Receive one multipart message, transparently answering PINGs and skipping other commands.
    pub async fn recv_multipart(&mut self) -> Result<Vec<Bytes>> { self.rx.recv().await }

    async fn send_command(&mut self, body: Bytes) -> Result<()> { self.rx.writer.reserve().await.command(body).await }
}

/// A multipart message or socket command from a ZMTP peer.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    /// One complete multipart message.
    Message(Vec<Bytes>),
    /// The peer subscribed to a topic prefix.
    Subscribe(Bytes),
    /// The peer cancelled a topic-prefix subscription.
    Cancel(Bytes),
    /// A ZMTP heartbeat request whose context should be returned with [`PeerWriter::pong`].
    Ping(Bytes),
}

/// An accepted ZMTP peer after its NULL-security handshake has completed.
pub struct Peer<S> { reader: PeerReader<ReadHalf<S>>, writer: PeerWriter<WriteHalf<S>>, peer_meta: Vec<(String, Bytes)> }

impl<S: AsyncRead + AsyncWrite + Unpin> Peer<S> {
    async fn handshake(mut s: S, server: bool, socket_type: &str, identity: Option<&[u8]>, max_frame: usize) -> Result<Self> {
        s.write_all(&encode_greeting(server)).await?;
        let mut greeting = [0; GREETING_LEN];
        s.read_exact(&mut greeting).await?;
        check_greeting(&greeting)?;
        let (r, w) = io::split(s);
        let mut reader = PeerReader { s: r, rbuf: BytesMut::new(), parts: vec![], max_frame };
        let mut writer = PeerWriter { s: w, wbuf: BytesMut::new() };
        writer.command(ready_command(socket_type, identity)).await?;
        let peer_meta = loop {
            let frame = reader.read_frame().await?;
            if !frame.command { return perr("message frame before handshake completed"); }
            match parse_command(&frame.body)? {
                Command::Ready(meta) => break meta, Command::Error(e) => return perr(format!("peer rejected handshake: {e}")),
                Command::Ping { context, .. } => writer.pong(&context).await?,
                Command::Subscribe(_) | Command::Cancel(_) => return perr("subscription command during handshake"), Command::Other => {}
            }
        };
        if let Some((_, t)) = peer_meta.iter().find(|(k, _)| k.eq_ignore_ascii_case("socket-type")) {
            let theirs = String::from_utf8_lossy(t);
            if !compatible(socket_type, &theirs) { return perr(format!("peer socket type {theirs} is incompatible with {socket_type}")); }
        }
        Ok(Self { reader, writer, peer_meta })
    }

    async fn accept(s: S, socket_type: &str) -> Result<Self> { Self::handshake(s, true, socket_type, None, DEFAULT_MAX_FRAME).await }

    /// Accept a DEALER or REQ peer as a ROUTER endpoint.
    pub async fn router(s: S) -> Result<Self> { Self::accept(s, "ROUTER").await }

    /// Accept a SUB peer as a PUB endpoint.
    pub async fn publisher(s: S) -> Result<Self> { Self::accept(s, "PUB").await }

    /// Accept a SUB peer as an XPUB endpoint.
    pub async fn xpublisher(s: S) -> Result<Self> { Self::accept(s, "XPUB").await }

    /// Accept a REQ or DEALER peer as a REP endpoint.
    pub async fn reply(s: S) -> Result<Self> { Self::accept(s, "REP").await }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { &self.peer_meta }

    /// The peer's ZMTP identity, when it supplied one.
    pub fn identity(&self) -> Option<&[u8]> { self.peer_meta().iter().find(|(k, _)| k.eq_ignore_ascii_case("identity")).map(|(_, value)| &value[..]) }

    /// Cap the body size of each frame received after the handshake, `DEFAULT_MAX_FRAME` until set. `split` keeps the cap.
    pub fn with_max_frame(mut self, max_frame: usize) -> Self { self.reader.max_frame = max_frame; self }

    /// Split this peer into independently driven receive and send halves.
    pub fn split(self) -> (PeerReader<ReadHalf<S>>, PeerWriter<WriteHalf<S>>) { (self.reader, self.writer) }
}

/// Receive half of an accepted ZMTP peer.
pub struct PeerReader<R> { s: R, rbuf: BytesMut, parts: Vec<Bytes>, max_frame: usize }

impl<R: AsyncRead + Unpin> PeerReader<R> {
    async fn read_frame(&mut self) -> Result<RawFrame> {
        loop {
            if let Some(frame) = decode_frame(&mut self.rbuf, self.max_frame)? { return Ok(frame); }
            if self.s.read_buf(&mut self.rbuf).await? == 0 {
                if self.rbuf.is_empty() && self.parts.is_empty() { return Err(Error::Closed); }
                return perr("connection closed during a multipart message");
            }
        }
    }

    /// Receive the next multipart message or relevant peer command.
    pub async fn recv(&mut self) -> Result<Incoming> {
        loop {
            let frame = self.read_frame().await?;
            if frame.command {
                match parse_command(&frame.body)? {
                    Command::Ping { context, .. } => return Ok(Incoming::Ping(context)), Command::Subscribe(topic) => return Ok(Incoming::Subscribe(topic)),
                    Command::Cancel(topic) => return Ok(Incoming::Cancel(topic)), Command::Error(e) => return perr(format!("peer error: {e}")),
                    Command::Ready(_) | Command::Other => continue
                }
            }
            let more = frame.more;
            self.parts.push(frame.body);
            if !more { return Ok(Incoming::Message(mem::take(&mut self.parts))); }
        }
    }
}

/// Send half of a ZMTP peer.
pub struct PeerWriter<W> { s: W, wbuf: BytesMut }

impl<W: AsyncWrite + Unpin> PeerWriter<W> {
    async fn flush(&mut self) -> Result<()> { self.s.write_all_buf(&mut self.wbuf).await?; Ok(()) }

    /// Send one multipart message.
    pub async fn send(&mut self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> {
        let mut parts = parts.into_iter().peekable();
        while let Some(part) = parts.next() { encode_frame(part.as_ref(), parts.peek().is_some(), false, &mut self.wbuf); }
        self.flush().await
    }

    /// Answer a peer PING command with its opaque context.
    pub async fn pong(&mut self, context: &[u8]) -> Result<()> { self.command(pong_command(context)).await }

    async fn command(&mut self, body: Bytes) -> Result<()> { encode_frame(&body, false, true, &mut self.wbuf); self.flush().await }
}

struct Outbound<S> { writer: Mutex<PeerWriter<WriteHalf<S>>>, started: Notify }

/// Exclusive access to a DEALER writer, acquired by [`DealerWriter::reserve`].
pub type DealerSend<'a, S = TcpStream> = MutexGuard<'a, PeerWriter<WriteHalf<S>>>;

/// Independently usable send half of a DEALER socket. Concurrent writes are serialized.
pub struct DealerWriter<S = TcpStream>(Arc<Outbound<S>>);

impl<S: AsyncWrite + Unpin> DealerWriter<S> {
    /// Send one multipart message.
    pub async fn send(&self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> { self.reserve().await.send(parts).await }

    /// Wait for exclusive access without staging a message. Cancelling this wait sends nothing.
    pub async fn reserve(&self) -> DealerSend<'_, S> {
        let writer = self.0.writer.lock().await;
        self.0.started.notify_one();
        writer
    }
}

/// Receive half of a connected DEALER or SUB socket, with transparent PING/PONG.
pub struct ClientReader<S = TcpStream> { reader: PeerReader<ReadHalf<S>>, writer: DealerWriter<S>, ping: Option<Bytes> }

impl<S: AsyncRead + AsyncWrite + Unpin> ClientReader<S> {
    async fn next(&mut self) -> Result<Incoming> {
        let out = &self.writer.0;
        // A sender can be cancelled while this read is already waiting. Finish its bytes without blocking incoming data.
        tokio::select! { biased;
            failed = async { loop {
                out.writer.lock().await.flush().await?;
                out.started.notified().await;
            } } => failed,
            msg = self.reader.recv() => msg,
        }
    }

    /// Receive one multipart message, retaining partial frames and pending PONGs across cancellation.
    pub async fn recv(&mut self) -> Result<Vec<Bytes>> {
        loop {
            if self.ping.is_none() {
                match self.next().await? {
                    Incoming::Message(msg) => return Ok(msg), Incoming::Ping(context) => self.ping = Some(context),
                    Incoming::Subscribe(_) | Incoming::Cancel(_) => continue,
                }
            }
            // Keep the PING until we can stage its PONG on the same writer as application messages.
            let mut writer = self.writer.0.writer.lock().await;
            writer.pong(&self.ping.take().expect("pending ping")).await?;
        }
    }
}

/// DEALER socket connected to a ROUTER, REP, or DEALER peer (e.g. a Jupyter kernel channel).
pub struct Dealer<S = TcpStream>(ZmtpStream<S>);

impl Dealer<TcpStream> {
    /// Connect over TCP (with `TCP_NODELAY`), optionally carrying an Identity.
    pub async fn connect(addr: impl ToSocketAddrs, identity: Option<&[u8]>) -> Result<Self> {
        let s = TcpStream::connect(addr).await?;
        s.set_nodelay(true)?;
        Self::from_stream(s, identity).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Dealer<S> {
    /// Handshake as DEALER over an established stream.
    pub async fn from_stream(s: S, identity: Option<&[u8]>) -> Result<Self> { Ok(Self(ZmtpStream::handshake(s, "DEALER", identity, DEFAULT_MAX_FRAME).await?)) }

    /// Split the socket. The reader answers PINGs, even when writes come from a different task.
    pub fn split(self) -> (ClientReader<S>, DealerWriter<S>) {
        let rx = self.0.rx;
        let tx = DealerWriter(rx.writer.0.clone());
        (rx, tx)
    }

    /// Send one multipart message.
    pub async fn send(&mut self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> { self.0.send_multipart(parts).await }

    /// Receive one multipart message.
    pub async fn recv(&mut self) -> Result<Vec<Bytes>> { self.0.recv_multipart().await }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { self.0.peer_meta() }

    /// Cap the body size of each frame received after the handshake, `DEFAULT_MAX_FRAME` until set.
    pub fn with_max_frame(mut self, max_frame: usize) -> Self { self.0.rx.reader.max_frame = max_frame; self }
}

/// SUB socket connected to an XPUB or PUB peer.
pub struct Sub<S = TcpStream>(ZmtpStream<S>);

impl Sub<TcpStream> {
    /// Connect over TCP (with `TCP_NODELAY`).
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let s = TcpStream::connect(addr).await?;
        s.set_nodelay(true)?;
        Self::from_stream(s).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sub<S> {
    /// Handshake as SUB over an established stream.
    pub async fn from_stream(s: S) -> Result<Self> { Ok(Self(ZmtpStream::handshake(s, "SUB", None, DEFAULT_MAX_FRAME).await?)) }

    /// Subscribe to messages whose first frame starts with `topic` (empty subscribes to everything).
    pub async fn subscribe(&mut self, topic: &[u8]) -> Result<()> { self.0.send_command(subscribe_command(topic)).await }

    /// Finish setting subscriptions and receive messages directly from this socket.
    pub fn into_reader(self) -> ClientReader<S> { self.0.rx }

    /// Receive one multipart message.
    pub async fn recv(&mut self) -> Result<Vec<Bytes>> { self.0.recv_multipart().await }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { self.0.peer_meta() }

    /// Cap the body size of each frame received after the handshake, `DEFAULT_MAX_FRAME` until set.
    pub fn with_max_frame(mut self, max_frame: usize) -> Self { self.0.rx.reader.max_frame = max_frame; self }
}

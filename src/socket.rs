//! Async ZMTP sockets: handshake, multipart send/receive, `Dealer` and `Sub`.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::wire::*;

/// Default cap on a single frame's body size (64 MiB).
pub const DEFAULT_MAX_FRAME: usize = 64 * 1024 * 1024;

/// Low-level ZMTP 3.1 connection over any async byte stream.
///
/// `recv_multipart` and `send_multipart` are cancellation-safe. All progress
/// (read buffer, partially received message, unflushed writes) lives in the
/// socket, so a dropped future loses nothing and the next call resumes. If a
/// call returns `Err`, the connection is unusable. Discard it.
pub struct ZmtpStream<S> {
    s: S,
    rbuf: BytesMut,
    wbuf: BytesMut,
    parts: Vec<Bytes>,
    max_frame: usize,
    peer_meta: Vec<(String, Bytes)>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ZmtpStream<S> {
    /// Perform the ZMTP 3.1 NULL handshake as `socket_type`, with optional Identity metadata.
    pub async fn handshake(mut s: S, socket_type: &str, identity: Option<&[u8]>, max_frame: usize) -> Result<Self> {
        s.write_all(&encode_greeting(false)).await?;
        let mut g = [0u8; GREETING_LEN];
        s.read_exact(&mut g).await?;
        check_greeting(&g)?;
        let mut ready = BytesMut::new();
        encode_frame(&ready_command(socket_type, identity), false, true, &mut ready);
        s.write_all(&ready).await?;
        let mut me = ZmtpStream { s, rbuf: BytesMut::new(), wbuf: BytesMut::new(), parts: vec![], max_frame, peer_meta: vec![] };
        loop {
            let f = me.read_frame().await?;
            if !f.command { return perr("message frame before handshake completed"); }
            match parse_command(&f.body)? {
                Command::Ready(meta) => {
                    me.peer_meta = meta;
                    break;
                }
                Command::Error(e) => return perr(format!("peer rejected handshake: {e}")),
                Command::Ping { context, .. } => me.stage_pong(&context).await?,
                Command::Subscribe(_) | Command::Cancel(_) => return perr("subscription command during handshake"),
                Command::Other => {}
            }
        }
        if let Some((_, t)) = me.peer_meta.iter().find(|(k, _)| k.eq_ignore_ascii_case("socket-type")) {
            let theirs = String::from_utf8_lossy(t).into_owned();
            if !compatible(socket_type, &theirs) { return perr(format!("peer socket type {theirs} is incompatible with {socket_type}")); }
        }
        Ok(me)
    }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { &self.peer_meta }

    async fn read_frame(&mut self) -> Result<RawFrame> {
        loop {
            if let Some(f) = decode_frame(&mut self.rbuf, self.max_frame)? { return Ok(f); }
            if self.s.read_buf(&mut self.rbuf).await? == 0 {
                if self.rbuf.is_empty() && self.parts.is_empty() { return Err(Error::Closed); }
                return perr("connection closed during a multipart message");
            }
        }
    }

    async fn flush(&mut self) -> Result<()> {
        while !self.wbuf.is_empty() { self.s.write_all_buf(&mut self.wbuf).await? }
        Ok(())
    }

    async fn stage_pong(&mut self, context: &[u8]) -> Result<()> {
        let pong = pong_command(context);
        encode_frame(&pong, false, true, &mut self.wbuf);
        self.flush().await
    }

    /// Send one multipart message.
    pub async fn send_multipart(&mut self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> {
        let parts: Vec<_> = parts.into_iter().collect();
        for (i, p) in parts.iter().enumerate() { encode_frame(p.as_ref(), i < parts.len() - 1, false, &mut self.wbuf); }
        self.flush().await
    }

    /// Receive one multipart message, transparently answering PINGs and skipping other commands.
    pub async fn recv_multipart(&mut self) -> Result<Vec<Bytes>> {
        self.flush().await?; // writes cancelled mid-send resume here
        loop {
            let f = self.read_frame().await?;
            if f.command {
                match parse_command(&f.body)? {
                    Command::Ping { context, .. } => self.stage_pong(&context).await?,
                    Command::Subscribe(_) | Command::Cancel(_) => {}
                    Command::Error(e) => return perr(format!("peer error: {e}")),
                    Command::Ready(_) | Command::Other => {}
                }
                continue;
            }
            let more = f.more;
            self.parts.push(f.body);
            if !more { return Ok(std::mem::take(&mut self.parts)); }
        }
    }

    pub(crate) async fn send_command(&mut self, body: Bytes) -> Result<()> {
        encode_frame(&body, false, true, &mut self.wbuf);
        self.flush().await
    }
}

/// A message or socket command received from an accepted peer.
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
pub struct Peer<S> { stream: ZmtpStream<S> }

impl<S: AsyncRead + AsyncWrite + Unpin> Peer<S> {
    async fn accept(mut s: S, socket_type: &str) -> Result<Self> {
        s.write_all(&encode_greeting(true)).await?;
        let mut greeting = [0u8; GREETING_LEN];
        s.read_exact(&mut greeting).await?;
        check_greeting(&greeting)?;

        let mut ready = BytesMut::new();
        encode_frame(&ready_command(socket_type, None), false, true, &mut ready);
        s.write_all(&ready).await?;
        let mut stream = ZmtpStream { s, rbuf: BytesMut::new(), wbuf: BytesMut::new(), parts: vec![], max_frame: DEFAULT_MAX_FRAME, peer_meta: vec![] };
        loop {
            let frame = stream.read_frame().await?;
            if !frame.command { return perr("message frame before handshake completed"); }
            match parse_command(&frame.body)? {
                Command::Ready(meta) => {
                    stream.peer_meta = meta;
                    break;
                }
                Command::Error(e) => return perr(format!("peer rejected handshake: {e}")),
                Command::Ping { context, .. } => stream.stage_pong(&context).await?,
                Command::Subscribe(_) | Command::Cancel(_) => return perr("subscription command during handshake"),
                Command::Other => {}
            }
        }
        if let Some((_, peer_type)) = stream.peer_meta.iter().find(|(k, _)| k.eq_ignore_ascii_case("socket-type")) {
            let peer_type = String::from_utf8_lossy(peer_type);
            if !compatible(socket_type, &peer_type) { return perr(format!("peer socket type {peer_type} is incompatible with {socket_type}")); }
        }
        Ok(Self { stream })
    }

    /// Accept a DEALER or REQ peer as a ROUTER endpoint.
    pub async fn router(s: S) -> Result<Self> { Self::accept(s, "ROUTER").await }

    /// Accept a SUB peer as a PUB endpoint.
    pub async fn publisher(s: S) -> Result<Self> { Self::accept(s, "PUB").await }

    /// Accept a SUB peer as an XPUB endpoint.
    pub async fn xpublisher(s: S) -> Result<Self> { Self::accept(s, "XPUB").await }

    /// Accept a REQ or DEALER peer as a REP endpoint.
    pub async fn reply(s: S) -> Result<Self> { Self::accept(s, "REP").await }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { &self.stream.peer_meta }

    /// The peer's ZMTP identity, when it supplied one.
    pub fn identity(&self) -> Option<&[u8]> { self.peer_meta().iter().find(|(k, _)| k.eq_ignore_ascii_case("identity")).map(|(_, value)| &value[..]) }

    /// Split this peer into independently driven receive and send halves.
    pub fn split(self) -> (PeerReader<ReadHalf<S>>, PeerWriter<WriteHalf<S>>) {
        let ZmtpStream { s, rbuf, wbuf, parts, max_frame, .. } = self.stream;
        let (reader, writer) = tokio::io::split(s);
        (PeerReader { s: reader, rbuf, parts, max_frame }, PeerWriter { s: writer, wbuf })
    }
}

/// Receive half of an accepted ZMTP peer.
pub struct PeerReader<R> {
    s: R,
    rbuf: BytesMut,
    parts: Vec<Bytes>,
    max_frame: usize,
}

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
                    Command::Ping { context, .. } => return Ok(Incoming::Ping(context)),
                    Command::Subscribe(topic) => return Ok(Incoming::Subscribe(topic)),
                    Command::Cancel(topic) => return Ok(Incoming::Cancel(topic)),
                    Command::Error(e) => return perr(format!("peer error: {e}")),
                    Command::Ready(_) | Command::Other => continue,
                }
            }
            let more = frame.more;
            self.parts.push(frame.body);
            if !more { return Ok(Incoming::Message(std::mem::take(&mut self.parts))); }
        }
    }
}

/// Send half of an accepted ZMTP peer.
pub struct PeerWriter<W> { s: W, wbuf: BytesMut }

impl<W: AsyncWrite + Unpin> PeerWriter<W> {
    async fn flush(&mut self) -> Result<()> {
        while !self.wbuf.is_empty() { self.s.write_all_buf(&mut self.wbuf).await?; }
        Ok(())
    }

    /// Send one multipart message.
    pub async fn send(&mut self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> {
        let parts: Vec<_> = parts.into_iter().collect();
        for (i, part) in parts.iter().enumerate() { encode_frame(part.as_ref(), i < parts.len() - 1, false, &mut self.wbuf); }
        self.flush().await
    }

    /// Answer a peer PING command with its opaque context.
    pub async fn pong(&mut self, context: &[u8]) -> Result<()> {
        encode_frame(&pong_command(context), false, true, &mut self.wbuf);
        self.flush().await
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

    /// Send one multipart message.
    pub async fn send(&mut self, parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> { self.0.send_multipart(parts).await }

    /// Receive one multipart message.
    pub async fn recv(&mut self) -> Result<Vec<Bytes>> { self.0.recv_multipart().await }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { self.0.peer_meta() }
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

    /// Receive one multipart message.
    pub async fn recv(&mut self) -> Result<Vec<Bytes>> { self.0.recv_multipart().await }

    /// Metadata properties from the peer's READY command.
    pub fn peer_meta(&self) -> &[(String, Bytes)] { self.0.peer_meta() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    async fn scripted_peer(socket_type: &str) -> (Dealer<tokio::io::DuplexStream>, tokio::io::DuplexStream) {
        let (client, mut server) = tokio::io::duplex(1 << 16);
        let mut setup = BytesMut::new();
        setup.extend_from_slice(&encode_greeting(true));
        encode_frame(&ready_command(socket_type, None), false, true, &mut setup);
        server.write_all(&setup).await.unwrap();
        (Dealer::from_stream(client, Some(b"tid")).await.unwrap(), server)
    }

    async fn read_peer_frame(server: &mut tokio::io::DuplexStream, buf: &mut BytesMut) -> RawFrame {
        loop {
            if let Some(f) = decode_frame(buf, DEFAULT_MAX_FRAME).unwrap() { return f; }
            assert!(server.read_buf(buf).await.unwrap() > 0, "peer closed");
        }
    }

    #[tokio::test]
    async fn handshake_and_robustness() {
        let (mut d, mut server) = scripted_peer("ROUTER").await;
        assert!(d.peer_meta().iter().any(|(k, v)| k.eq_ignore_ascii_case("socket-type") && &v[..] == b"ROUTER"));

        // the peer got our greeting, then a READY carrying our Identity
        let mut pbuf = BytesMut::new();
        let mut g = [0u8; GREETING_LEN];
        server.read_exact(&mut g).await.unwrap();
        check_greeting(&g).unwrap();
        let f = read_peer_frame(&mut server, &mut pbuf).await;
        assert!(f.command);
        match parse_command(&f.body).unwrap() {
            Command::Ready(meta) => assert!(meta.iter().any(|(k, v)| k == "Identity" && &v[..] == b"tid")),
            c => panic!("expected Ready, got {c:?}"),
        }

        // fragmented frame: a cancelled recv loses nothing, the retry returns the whole message
        let mut fr = BytesMut::new();
        encode_frame(b"hello", false, false, &mut fr);
        server.write_all(&fr[..3]).await.unwrap();
        assert!(timeout(Duration::from_millis(20), d.recv()).await.is_err()); // recv future dropped mid-frame
        server.write_all(&fr[3..]).await.unwrap();
        assert!(d.recv().await.unwrap() == vec![Bytes::from_static(b"hello")]);

        // multipart with a PING interleaved between parts: parts intact, PONG answered
        let mut fr = BytesMut::new();
        encode_frame(b"p1", true, false, &mut fr);
        let mut ping = BytesMut::new();
        ping.extend_from_slice(b"\x04PING\x00\x64ctx");
        encode_frame(&ping, false, true, &mut fr);
        encode_frame(b"p2", false, false, &mut fr);
        server.write_all(&fr).await.unwrap();
        assert!(d.recv().await.unwrap() == vec![Bytes::from_static(b"p1"), Bytes::from_static(b"p2")]);
        let f = read_peer_frame(&mut server, &mut pbuf).await;
        assert!(f.command && &f.body[..] == b"\x04PONGctx");

        // cancelled send resumes: both messages arrive whole and in order
        let big = vec![7u8; 1 << 20]; // larger than the duplex buffer, so send must block mid-write
        let sent = timeout(Duration::from_millis(20), d.send([&big[..]])).await;
        assert!(sent.is_err()); // send future dropped mid-flush
        // draining the peer side lets the resumed flush finish
        let reader = tokio::spawn(async move {
            let mut pbuf = BytesMut::new();
            let f1 = read_peer_frame(&mut server, &mut pbuf).await;
            let f2 = read_peer_frame(&mut server, &mut pbuf).await;
            (f1, f2)
        });
        d.send([b"after".as_ref()]).await.unwrap();
        let (f1, f2) = reader.await.unwrap();
        assert!(f1.body.len() == 1 << 20 && &f2.body[..] == b"after");

        // ordinary EOF is distinct from a protocol violation
        let (mut d, server) = scripted_peer("ROUTER").await;
        drop(server);
        assert!(matches!(d.recv().await, Err(Error::Closed)));

        let (mut d, mut server) = scripted_peer("ROUTER").await;
        server.write_all(b"\0\x05x").await.unwrap();
        drop(server);
        assert!(matches!(d.recv().await, Err(Error::Protocol(_))));
    }

    #[tokio::test]
    async fn handshake_rejects_wrong_peer() {
        let (client, mut server) = tokio::io::duplex(1 << 16);
        let mut setup = BytesMut::new();
        setup.extend_from_slice(&encode_greeting(true));
        encode_frame(&ready_command("PUB", None), false, true, &mut setup);
        server.write_all(&setup).await.unwrap();
        let e = Dealer::from_stream(client, None).await.err().expect("handshake should have failed");
        assert!(e.to_string().contains("PUB"));
    }

    #[tokio::test]
    async fn accepted_peers_round_trip_and_split() {
        let (client, server) = tokio::io::duplex(1 << 16);
        let (dealer, peer) = tokio::join!(Dealer::from_stream(client, Some(b"client-1")), Peer::router(server));
        let mut dealer = dealer.unwrap();
        let peer = peer.unwrap();
        assert_eq!(peer.identity(), Some(&b"client-1"[..]));
        let (mut reader, mut writer) = peer.split();

        dealer.send([b"one".as_ref(), b"two".as_ref()]).await.unwrap();
        assert_eq!(reader.recv().await.unwrap(), Incoming::Message(vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")]));
        writer.send([b"reply".as_ref()]).await.unwrap();
        assert_eq!(dealer.recv().await.unwrap(), vec![Bytes::from_static(b"reply")]);
        drop(dealer);
        assert!(matches!(reader.recv().await, Err(Error::Closed)));

        let (client, server) = tokio::io::duplex(1 << 16);
        let (sub, peer) = tokio::join!(Sub::from_stream(client), Peer::xpublisher(server));
        let mut sub = sub.unwrap();
        let (mut reader, mut writer) = peer.unwrap().split();
        sub.subscribe(b"topic").await.unwrap();
        assert_eq!(reader.recv().await.unwrap(), Incoming::Subscribe(Bytes::from_static(b"topic")));
        writer.send([b"topic".as_ref(), b"payload".as_ref()]).await.unwrap();
        assert_eq!(sub.recv().await.unwrap(), vec![Bytes::from_static(b"topic"), Bytes::from_static(b"payload")]);
    }
}

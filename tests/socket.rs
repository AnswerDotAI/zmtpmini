use bytes::{Bytes, BytesMut};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::time::{Duration, timeout};
use zmtpmini::wire::{Command, GREETING_LEN, RawFrame, check_greeting, decode_frame, encode_frame, encode_greeting, parse_command, ready_command};
use zmtpmini::{DEFAULT_MAX_FRAME, Dealer, Error, Incoming, Peer, Sub};

async fn scripted_peer(socket_type: &str) -> (Dealer<DuplexStream>, DuplexStream) {
    let (client, mut server) = io::duplex(1 << 16);
    let mut setup = BytesMut::new();
    setup.extend_from_slice(&encode_greeting(true));
    encode_frame(&ready_command(socket_type, None), false, true, &mut setup);
    server.write_all(&setup).await.unwrap();
    (Dealer::from_stream(client, Some(b"tid")).await.unwrap(), server)
}

async fn read_peer_frame(server: &mut DuplexStream, buf: &mut BytesMut) -> RawFrame {
    loop {
        if let Some(f) = decode_frame(buf, DEFAULT_MAX_FRAME).unwrap() { return f; }
        assert!(server.read_buf(buf).await.unwrap() > 0, "peer closed");
    }
}

#[tokio::test]
async fn handshake_and_robustness() {
    let (mut d, mut server) = scripted_peer("ROUTER").await;
    assert!(d.peer_meta().iter().any(|(k, v)| k.eq_ignore_ascii_case("socket-type") && &v[..] == b"ROUTER"));

    let mut pbuf = BytesMut::new();
    let mut g = [0u8; GREETING_LEN];
    server.read_exact(&mut g).await.unwrap();
    check_greeting(&g).unwrap();
    let f = read_peer_frame(&mut server, &mut pbuf).await;
    assert!(f.command);
    match parse_command(&f.body).unwrap() {
        Command::Ready(meta) => assert!(meta.iter().any(|(k, v)| k == "Identity" && &v[..] == b"tid")), c => panic!("expected Ready, got {c:?}")
    }

    let mut fr = BytesMut::new();
    encode_frame(b"hello", false, false, &mut fr);
    server.write_all(&fr[..3]).await.unwrap();
    assert!(timeout(Duration::from_millis(20), d.recv()).await.is_err());
    let (mut rx, tx) = d.split();
    server.write_all(&fr[3..]).await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), [Bytes::from_static(b"hello")]);

    let mut fr = BytesMut::new();
    encode_frame(b"p1", true, false, &mut fr);
    let mut ping = BytesMut::new();
    ping.extend_from_slice(b"\x04PING\x00\x64ctx");
    encode_frame(&ping, false, true, &mut fr);
    encode_frame(b"p2", false, false, &mut fr);
    server.write_all(&fr).await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), [Bytes::from_static(b"p1"), Bytes::from_static(b"p2")]);
    let f = read_peer_frame(&mut server, &mut pbuf).await;
    assert!(f.command && &f.body[..] == b"\x04PONGctx");

    let big = vec![7u8; 1 << 20];
    assert!(timeout(Duration::from_millis(20), tx.send([&big[..]])).await.is_err()); // pipe full, send cancelled
    let mut fr = BytesMut::new();
    encode_frame(b"after cancel", false, false, &mut fr);
    encode_frame(b"while sending", false, false, &mut fr);
    encode_frame(&ping, false, true, &mut fr);
    encode_frame(b"after ping", false, false, &mut fr);
    server.write_all(&fr).await.unwrap();
    let wait = Duration::from_secs(1);
    assert_eq!(timeout(wait, rx.recv()).await.unwrap().unwrap(), [Bytes::from_static(b"after cancel")]);
    let mut send = Box::pin(tx.send([b"trailer"]));
    assert!(timeout(Duration::from_millis(20), &mut send).await.is_err()); // writer still held
    assert_eq!(timeout(wait, rx.recv()).await.unwrap().unwrap(), [Bytes::from_static(b"while sending")]);
    assert!(timeout(Duration::from_millis(20), rx.recv()).await.is_err()); // PONG waits behind the blocked write
    drop(send);
    let drain = async {
        (read_peer_frame(&mut server, &mut pbuf).await, read_peer_frame(&mut server, &mut pbuf).await, read_peer_frame(&mut server, &mut pbuf).await)
    };
    let (msg, (data, trailer, pong)) = timeout(Duration::from_secs(1), async { tokio::join!(rx.recv(), drain) }).await.unwrap();
    assert_eq!(msg.unwrap(), [Bytes::from_static(b"after ping")]);
    assert!(&data.body[..] == big);
    assert_eq!(&trailer.body[..], b"trailer");
    assert!(pong.command && &pong.body[..] == b"\x04PONGctx");

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
    let (client, mut server) = io::duplex(1 << 16);
    let mut setup = BytesMut::new();
    setup.extend_from_slice(&encode_greeting(true));
    encode_frame(&ready_command("PUB", None), false, true, &mut setup);
    server.write_all(&setup).await.unwrap();
    let e = Dealer::from_stream(client, None).await.err().expect("handshake should have failed");
    assert!(e.to_string().contains("PUB"));
}

#[tokio::test]
async fn accepted_peers_round_trip_and_split() {
    let (client, server) = io::duplex(1 << 16);
    let (dealer, peer) = tokio::join!(Dealer::from_stream(client, Some(b"client-1")), Peer::router(server));
    let (mut rx, tx) = dealer.unwrap().split();
    let peer = peer.unwrap();
    assert_eq!(peer.identity(), Some(&b"client-1"[..]));
    let (mut reader, mut writer) = peer.split();

    let payload = Bytes::from(vec![2; 1 << 17]);
    let mut reply = Box::pin(rx.recv());
    assert!(timeout(Duration::from_millis(20), &mut reply).await.is_err()); // reader waiting before the send
    assert!(timeout(Duration::from_millis(20), tx.send([&b"one"[..], &payload])).await.is_err());
    let answer = async {
        assert!(matches!(reader.recv().await.unwrap(), Incoming::Message(parts) if parts == [Bytes::from_static(b"one"), payload]));
        writer.send([b"reply"]).await.unwrap();
    };
    let (msg, ()) = timeout(Duration::from_secs(1), async { tokio::join!(reply, answer) }).await.unwrap();
    assert_eq!(msg.unwrap(), [Bytes::from_static(b"reply")]);
    drop((rx, tx));
    assert!(matches!(reader.recv().await, Err(Error::Closed)));

    let (client, server) = io::duplex(1 << 16);
    let (sub, peer) = tokio::join!(Sub::from_stream(client), Peer::xpublisher(server));
    let mut sub = sub.unwrap().with_max_frame(16);
    let (mut reader, mut writer) = peer.unwrap().with_max_frame(16).split();
    sub.subscribe(b"topic").await.unwrap();
    assert_eq!(reader.recv().await.unwrap(), Incoming::Subscribe(Bytes::from_static(b"topic")));
    writer.send([b"topic".as_ref(), b"payload".as_ref()]).await.unwrap();
    assert_eq!(sub.recv().await.unwrap(), vec![Bytes::from_static(b"topic"), Bytes::from_static(b"payload")]);

    sub.subscribe(b"toolong").await.unwrap();
    assert!(reader.recv().await.unwrap_err().to_string().contains("exceeds cap of 16"));
    writer.send([b"payload is too long".as_ref()]).await.unwrap();
    assert!(sub.recv().await.unwrap_err().to_string().contains("exceeds cap of 16"));
}

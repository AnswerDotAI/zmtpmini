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
    server.write_all(&fr[3..]).await.unwrap();
    assert!(d.recv().await.unwrap() == vec![Bytes::from_static(b"hello")]);

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

    let big = vec![7u8; 1 << 20];
    let sent = timeout(Duration::from_millis(20), d.send([&big[..]])).await;
    assert!(sent.is_err());
    let reader = tokio::spawn(async move {
        let mut pbuf = BytesMut::new();
        let f1 = read_peer_frame(&mut server, &mut pbuf).await;
        let f2 = read_peer_frame(&mut server, &mut pbuf).await;
        (f1, f2)
    });
    d.send([b"after".as_ref()]).await.unwrap();
    let (f1, f2) = reader.await.unwrap();
    assert!(f1.body.len() == 1 << 20 && &f2.body[..] == b"after");

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

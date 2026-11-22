use bytes::{BufMut, BytesMut};
use zmtpmini::wire::{
    Command, cancel_command, check_greeting, compatible, decode_frame, encode_frame, encode_greeting, parse_command, pong_command, ready_command,
    subscribe_command,
};

#[test]
fn frame_walk() {
    let mut out = BytesMut::new();
    encode_frame(b"hi", true, false, &mut out);
    encode_frame(&[7u8; 300], false, false, &mut out);
    encode_frame(b"\x05READY", false, true, &mut out);
    let mut buf = BytesMut::new();

    buf.extend_from_slice(&out[..1]);
    assert!(decode_frame(&mut buf, 1 << 20).unwrap().is_none());
    buf.extend_from_slice(&out[1..]);

    let f = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert!((f.more, f.command, &f.body[..]) == (true, false, b"hi"));
    let f = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert!(!f.more && !f.command && f.body.len() == 300);
    let f = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert!(f.command && &f.body[..] == b"\x05READY");
    assert!(buf.is_empty());

    let mut big = BytesMut::new();
    encode_frame(&[0u8; 300], false, false, &mut big);
    assert!(decode_frame(&mut big, 100).is_err());

    let mut bad = BytesMut::from(&[0x10u8, 0x00][..]);
    assert!(decode_frame(&mut bad, 100).is_err());
}

#[test]
fn greeting_and_commands() {
    let g = encode_greeting(false);
    assert!(check_greeting(&g).is_ok());

    let mut old = g;
    old[11] = 0;
    let e = check_greeting(&old).unwrap_err();
    assert!(e.to_string().contains("3.0"));

    let mut curve = g;
    curve[12..17].copy_from_slice(b"CURVE");
    assert!(check_greeting(&curve).is_err());

    let ready = ready_command("DEALER", Some(b"sess1"));
    match parse_command(&ready).unwrap() {
        Command::Ready(meta) => {
            assert!(meta.iter().any(|(k, v)| k.eq_ignore_ascii_case("socket-type") && &v[..] == b"DEALER"));
            assert!(meta.iter().any(|(k, v)| k.eq_ignore_ascii_case("identity") && &v[..] == b"sess1"));
        }
        c => panic!("expected Ready, got {c:?}"),
    }

    let mut ping = BytesMut::new();
    ping.put_u8(4);
    ping.put_slice(b"PING");
    ping.put_u16(100);
    ping.put_slice(b"ctx");
    match parse_command(&ping.freeze()).unwrap() {
        Command::Ping { ttl, context } => assert!(ttl == 100 && &context[..] == b"ctx"), c => panic!("expected Ping, got {c:?}")
    }

    let pong = pong_command(b"ctx");
    assert!(pong.starts_with(b"\x04PONG") && pong.ends_with(b"ctx"));
    let sub = subscribe_command(b"");
    assert!(&sub[..] == b"\x09SUBSCRIBE");
    assert!(matches!(parse_command(&sub).unwrap(), Command::Subscribe(t) if t.is_empty()));
    assert!(matches!(parse_command(&cancel_command(b"abc")).unwrap(), Command::Cancel(t) if &t[..] == b"abc"));

    let mut other = BytesMut::new();
    other.put_u8(5);
    other.put_slice(b"HELLO");
    assert!(matches!(parse_command(&other.freeze()).unwrap(), Command::Other));

    assert!(compatible("DEALER", "ROUTER") && compatible("SUB", "XPUB") && compatible("SUB", "PUB"));
    assert!(!compatible("DEALER", "PUB") && !compatible("SUB", "ROUTER"));
}

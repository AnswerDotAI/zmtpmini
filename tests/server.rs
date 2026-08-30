//! The accepting-side story against unmodified pyzmq DEALER, SUB, and REQ peers.

use std::process::Command;
use std::time::Duration;
use tokio::net::TcpListener;
use zmtpmini::{Incoming, Peer};

fn endpoint(listener: &TcpListener) -> String { format!("tcp://{}", listener.local_addr().unwrap()) }

#[tokio::test]
async fn pyzmq_peer_story() {
    let router = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let publisher = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reply = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let python = std::env::var("ZMTPMINI_TEST_PYTHON").unwrap_or_else(|_| "python".into());
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pyzmq_peer.py");
    let mut child = Command::new(python)
        .arg(script)
        .args([endpoint(&router), endpoint(&publisher), endpoint(&reply)])
        .spawn()
        .expect("spawning pyzmq peer failed: is pyzmq installed?");

    let router_story = async {
        let (stream, _) = router.accept().await.unwrap();
        let peer = Peer::router(stream).await.unwrap();
        assert_eq!(peer.identity(), Some(&b"py-client"[..]));
        let (mut reader, mut writer) = peer.split();
        assert_eq!(reader.recv().await.unwrap(), Incoming::Message(vec![b"one".as_ref().into(), b"two".as_ref().into()]));
        writer.send([b"reply".as_ref()]).await.unwrap();
    };
    let publisher_story = async {
        let (stream, _) = publisher.accept().await.unwrap();
        let (mut reader, mut writer) = Peer::xpublisher(stream).await.unwrap().split();
        assert_eq!(reader.recv().await.unwrap(), Incoming::Subscribe(b"topic".as_ref().into()));
        writer.send([b"topic".as_ref(), b"payload".as_ref()]).await.unwrap();
    };
    let reply_story = async {
        let (stream, _) = reply.accept().await.unwrap();
        let (mut reader, mut writer) = Peer::reply(stream).await.unwrap().split();
        let Incoming::Message(message) = reader.recv().await.unwrap() else { panic!("expected heartbeat message") };
        writer.send(message).await.unwrap();
    };

    if tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(router_story, publisher_story, reply_story) }).await.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("pyzmq peer timed out");
    }
    assert!(child.wait().unwrap().success());
}

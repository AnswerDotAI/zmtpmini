//! The whole client story against a real kernel: handshake metadata, a shell
//! round trip, and iopub subscription proven by the JEP 65 welcome.

mod common;
use common::{jmsg, parse_jmsg, within};

#[tokio::test]
async fn kernel_story() {
    let kernel = common::Kernel::spawn();
    let session = "zmtpmini-test-session";

    // DEALER handshake against real libzmq: peer announces itself as ROUTER
    let mut shell = within(kernel.shell(session.as_bytes())).await;
    assert!(
        shell
            .peer_meta()
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("socket-type") && &v[..] == b"ROUTER")
    );

    // SUB handshake + subscribe-all; the welcome proves the subscription reached the XPUB
    let mut iopub = within(kernel.iopub()).await;
    iopub.subscribe(b"").await.unwrap();
    let (header, _, _) = parse_jmsg(&within(iopub.recv()).await.unwrap());
    assert_eq!(header["msg_type"], "iopub_welcome");

    // kernel_info round trip on shell
    shell
        .send(jmsg("kernel_info_request", session))
        .await
        .unwrap();
    let (header, parent, content) = parse_jmsg(&within(shell.recv()).await.unwrap());
    assert_eq!(header["msg_type"], "kernel_info_reply");
    assert_eq!(parent["session"], session);
    assert_eq!(content["status"], "ok");

    // ... which also published busy then idle statuses on iopub, tied to our session
    let mut states = vec![];
    while states.len() < 2 {
        let (header, parent, content) = parse_jmsg(&within(iopub.recv()).await.unwrap());
        if header["msg_type"] == "status" && parent["session"] == session {
            states.push(content["execution_state"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(states, ["busy", "idle"]);
}

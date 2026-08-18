//! Shared ipymini fixture and a minimal Jupyter wire-message layer (empty key, so no signing).

use bytes::Bytes;
use std::net::TcpListener;
use std::process::{Child, Command};
use std::time::Duration;
use zmtpmini::{Dealer, Sub};

/// A spawned ipymini kernel; killed on drop.
pub struct Kernel {
    child: Child,
    conn_file: std::path::PathBuf,
    /// shell channel address
    pub shell: String,
    /// iopub channel address
    pub iopub: String,
}

fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    listeners.iter().map(|l| l.local_addr().unwrap().port()).collect()
}

impl Kernel {
    /// Spawn an ipymini kernel with a fresh connection file (empty key: unsigned messages).
    pub fn spawn() -> Kernel {
        let p = free_ports(5);
        let conn = serde_json::json!({
            "transport": "tcp", "ip": "127.0.0.1", "key": "", "signature_scheme": "hmac-sha256",
            "shell_port": p[0], "iopub_port": p[1], "stdin_port": p[2], "control_port": p[3], "hb_port": p[4],
        });
        let conn_file = std::env::temp_dir().join(format!("zmtpmini-test-{}-{}.json", std::process::id(), p[0]));
        std::fs::write(&conn_file, conn.to_string()).unwrap();
        let python = std::env::var("ZMTPMINI_TEST_PYTHON").unwrap_or("python".into());
        let child = Command::new(python)
            .args(["-m", "ipymini", "-f"])
            .arg(&conn_file)
            .spawn()
            .expect("spawning ipymini failed: is it installed? (pip install ipymini)");
        Kernel { child, conn_file, shell: format!("127.0.0.1:{}", p[0]), iopub: format!("127.0.0.1:{}", p[1]) }
    }

    /// Connect a DEALER to the shell channel, retrying until the kernel is listening.
    pub async fn shell(&self, identity: &[u8]) -> Dealer {
        retry(|| Dealer::connect(&self.shell, Some(identity))).await
    }

    /// Connect a SUB to iopub, retrying until the kernel is listening.
    pub async fn iopub(&self) -> Sub {
        retry(|| Sub::connect(&self.iopub)).await
    }
}

impl Drop for Kernel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.conn_file);
    }
}

async fn retry<T, F: Future<Output = zmtpmini::Result<T>>>(mut f: impl FnMut() -> F) -> T {
    let end = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) => {
                assert!(std::time::Instant::now() < end, "kernel did not come up: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Await `f` with the standard test timeout, so a protocol bug fails instead of hanging.
pub async fn within<T, F: Future<Output = T>>(f: F) -> T {
    tokio::time::timeout(Duration::from_secs(30), f).await.expect("timed out")
}

/// Build an unsigned Jupyter wire message with an empty parent and content.
pub fn jmsg(msg_type: &str, session: &str) -> Vec<Bytes> {
    let header = serde_json::json!({
        "msg_id": format!("{}-{}", msg_type, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()),
        "session": session, "username": "test", "date": "2026-01-01T00:00:00Z",
        "msg_type": msg_type, "version": "5.3",
    });
    [&b"<IDS|MSG>"[..], b"", header.to_string().as_bytes(), b"{}", b"{}", b"{}"].map(Bytes::copy_from_slice).to_vec()
}

/// Parse a Jupyter wire message (skipping any leading topic frames): (header, parent, content).
pub fn parse_jmsg(frames: &[Bytes]) -> (serde_json::Value, serde_json::Value, serde_json::Value) {
    let i = frames.iter().position(|f| &f[..] == b"<IDS|MSG>").expect("no <IDS|MSG> delimiter");
    let v = |j: &Bytes| serde_json::from_slice(j).unwrap();
    (v(&frames[i + 2]), v(&frames[i + 3]), v(&frames[i + 5]))
}

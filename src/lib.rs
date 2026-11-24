//! ZMTP 3.1 multipart I/O over Tokio streams: connect-side DEALER and SUB, plus accepted ROUTER, PUB, XPUB and REP peers.
//! The sockets use NULL security. They do not listen or reconnect. Partial reads and staged writes survive cancellation.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod socket;
pub mod wire;

pub use socket::{ClientReader, DEFAULT_MAX_FRAME, Dealer, DealerSend, DealerWriter, Incoming, Peer, PeerReader, PeerWriter, Sub, ZmtpStream};
pub use wire::{Error, Result};

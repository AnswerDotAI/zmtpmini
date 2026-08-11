//! Minimal async ZMTP 3.1 client: connect-side DEALER and SUB sockets.
//!
//! Deliberately absent: bind, other socket types, CURVE/PLAIN security, ZMTP
//! 1.0/2.0/3.0 downgrade, and automatic reconnection. The scope is exactly what a
//! Jupyter-style client needs: connect to a peer's ROUTER or XPUB socket over TCP
//! (or any async byte stream), speak ZMTP 3.1 with NULL security, and exchange
//! multipart messages. All operations are cancellation-safe. A dropped future
//! never loses or tears a message, and the next call resumes cleanly.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod socket;
mod wire;

pub use socket::{DEFAULT_MAX_FRAME, Dealer, Sub, ZmtpStream};
pub use wire::{Error, Result};

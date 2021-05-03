//! # RustMQ
//!
//! # Introduction
//! RustMQ is an IPC (inter-process communication) framework written using minimal dependencies for the Rust language.
//! Intended to be easily extended and customized, but also provides its own implementations.
//!
//! ## Features
//! * Communication module (OSI 5 session layer) implementation for the following communication models:
//!   * Request-Reply (client-server) model
//!   * Publisher-Subscriber
//!   * Easily extendable with additional models
//! * Communication modules (OSI 1-4) implemented:
//!   * Only Network (IPv4 and IPv6) TCP and SSL is implemented at this point
//!
//! ## Example usage
//! ```rust
//! use rustymq::model::{RequestSocket, ReplySocket};
//! use rustymq::transport::network::tcp;
//! use rustymq::base::{Message, RawMessage, OpFlag, Socket,
//!                     InwardSocket, OutwardSocket, BidirectionalSocket,
//!                     NetworkAddress};
//!
//! fn main() {
//!     let requestor = RequestSocket::new(
//!                         tcp::InitiatorTransport::new(
//!                             tcp::StreamConnectionBuilder::new()))
//!                     .unwrap();
//!     let replier = ReplySocket::new(
//!                     tcp::AcceptorTransport::new(
//!                         tcp::StreamConnectionBuilder::new(),
//!                         tcp::StreamListenerBuilder::new()))
//!                   .unwrap();
//!     
//!     replier.bind(NetworkAddress::from_dns("localhost:45322".to_string()).unwrap().into()).unwrap();
//!     requestor.connect(NetworkAddress::from_dns("localhost:45322".to_string()).unwrap().into()).unwrap();
//!     
//!     let payload:Vec<u8> = vec![0u8, 5u8];
//!     let message = RawMessage::new(payload.clone());
//!     
//!     requestor.send(message, OpFlag::NoWait).unwrap();
//!     
//!     replier.respond(OpFlag::Wait, OpFlag::Wait, |rmessage:RawMessage| {
//!         RawMessage::new(rmessage.payload().clone()).continue_exchange_metadata(rmessage.into_metadata())
//!     }).unwrap();
//!     
//!     assert_eq!(payload, requestor.receive(OpFlag::Wait).unwrap().into_payload());
//! }
//! ```

/// Used for random message identifier generation
extern crate rand;

/// Used for SSL communication as an optional enableable feature
#[cfg(feature = "network-openssl-socket-support")]
extern crate openssl;

pub mod base;
pub mod internals;
pub mod model;
pub mod transport;

#[cfg(test)]
mod tests;

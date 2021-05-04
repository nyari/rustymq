//! # Network transport module
//! This module contains built-in implementations of **rustymq** for transport over TCP/IP networks.
//! Custom implementations can also be used instead of build in ones

pub mod common;

#[cfg(feature = "network-openssl-socket-support")]
pub mod ssl;
pub mod tcp;

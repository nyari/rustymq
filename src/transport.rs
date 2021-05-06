//! # Transport module
//! Contains already implemented transport layers (OSI Layers 1-4) that **rustymq** natively supports.
//! External implementations can also be used
pub mod network;

#[cfg(all(target_family = "unix", feature = "systemv-support"))]
pub mod systemv;
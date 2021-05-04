//! # Model module
//! This module contains sockets that implement a session layer (OSI layer 5) for a given
//! communication model. It is possible to use external implementations as well instead

pub mod pubsub;
pub mod reqrep;

pub use self::pubsub::{PublisherSocket, SubscriberSocket};
pub use self::reqrep::{ReplySocket, RequestSocket};

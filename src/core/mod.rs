//! # Core module
//! Core datastructures, traits and enums for use in RustyMQ

pub mod message;
pub mod socket;
pub mod transport;
pub mod serializer;

#[allow(dead_code)]
pub mod util;

pub use self::message::MessageMetadata;
pub use self::message::Message;
pub use self::message::RawMessage;
pub use self::message::TypedMessage;
pub use self::message::MessageId;
pub use self::message::ConversationId;
pub use self::message::PeerId;
pub use self::message::Part;
pub use self::message::PartError;

pub use self::socket::*;

pub use self::transport::TransportMethod;
pub use self::transport::NetworkAddress;
//! # Base module
//! Core data structures, enums and traits needed for usage of RustyMq

pub mod config;
pub mod info;
pub mod message;
pub mod queue;
pub mod socket;
pub mod transport;

pub use self::config::TransportConfiguration;
pub use self::info::Identifier;
pub use self::message::{
    Buffer, BufferMutSlice, BufferSlice, ConversationId, Message, MessageId, MessageMetadata, Part,
    PartError, PeerId, RawMessage, SerializableMessagePayload, TypedMessage,
};
pub use self::queue::{MessageQueueError, MessageQueueOverflowHandling, MessageQueueingPolicy};
pub use self::socket::{
    ArcSocket, BidirectionalSocket, InwardSocket, OpFlag, OutwardSocket, PeerIdentification,
    QueryTypedError, ReceiveTypedError, SendTypedError, Socket, SocketError,
};
pub use self::transport::{
    AcceptorTransport, BidirectionalTransport, InitiatorTransport, NetworkAddress, Transport,
    TransportMethod,
};

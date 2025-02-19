//! # Transport base module
//! Module containing interface and type definitions for transport methods that can be implemented in RustyMQ
pub mod network;

pub use self::network::NetworkAddress;

use crate::base::config::TransportConfiguration;
use crate::base::message::{PeerId, RawMessage};
use crate::base::socket::{OpFlag, PeerIdentification, SocketError};

use std::any;
use std::collections::HashSet;

/// # TransportMethod
/// TransportMethod should contain the data for a transport layer to be able to establish a connection
#[derive(Debug)]
pub enum TransportMethod {
    /// Connect through network
    Network(NetworkAddress),
    /// Allow implementation of custom transport methods to be used by custom [`Transport`] implementation
    Custom(Box<dyn any::Any>),
}

impl From<NetworkAddress> for TransportMethod {
    fn from(value: NetworkAddress) -> Self {
        TransportMethod::Network(value)
    }
}

/// # Transport
/// Trait that every transport layer implementation has to implement. This is the interface a Socket uses internally
/// to establis a connection
pub trait Transport: Send + Sync {
    /// Send message
    fn send(&self, message: RawMessage, flags: OpFlag) -> Result<(), SocketError>;
    /// Receive message
    fn receive(&self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)>;
    /// Close connection to the peer identified by [`PeerIdentification`]
    fn close_connection(
        &self,
        peer_identification: PeerIdentification,
    ) -> Result<Option<PeerId>, SocketError>;
    /// Query the [`PeerId`]s of all the connected peers
    fn query_connected_peers(&self) -> HashSet<PeerId>;
    /// Query the stored configuration if present
    fn query_configuration(&self) -> Option<&TransportConfiguration>;
    /// Close the connection to all connected peers
    fn close(self) -> Result<(), SocketError>;
}

/// # InitiatorTransport
/// Trait that every transport layer has to implement that can establish a connection
pub trait InitiatorTransport: Transport {
    /// Establist connection to peer defined by TransportMethod
    fn connect(&self, target: TransportMethod) -> Result<Option<PeerId>, SocketError>;
}

/// # AcceptorTransport
/// Trait that every transport layer has to implement that can listen for connections
pub trait AcceptorTransport: Transport {
    /// Listen for connections from peers on the TransportMethod defined
    fn bind(&self, target: TransportMethod) -> Result<Option<PeerId>, SocketError>;
}

/// # BidirectionalTransport
/// Trait that every transport layer has to implenent that can both establish connections and listen for them
pub trait BidirectionalTransport: InitiatorTransport + AcceptorTransport {}

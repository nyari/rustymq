//! # Publisher-Subscriber communication model
//! This module contains the socket implementation for a publisher-subscriber communication model.
//! ## Operation
//! The [`PublisherSocket`] is only an outward communication socket that can accept connections and every sent message
//! will be distributed the all the connecting peers with a [`SubscriberSocket`] 
//! ## Example
//! ```rust
//! use rustymq::core::socket::{Socket, InwardSocket, OutwardSocket, OpFlag};
//! use rustymq::core::message::{Message, RawMessage}; 
//! use rustymq::core::transport::{TransportMethod, NetworkAddress};
//! use rustymq::model::pubsub::{PublisherSocket, SubscriberSocket};
//! use rustymq::transport::network::tcp;
//! # fn main() {
//! 
//! let mut publisher = PublisherSocket::new(tcp::AcceptorTransport::new(tcp::StreamConnectionBuilder::new(), tcp::StreamListenerBuilder::new()));
//! let mut subscriber1 = SubscriberSocket::new(tcp::InitiatorTransport::new(tcp::StreamConnectionBuilder::new()));
//! let mut subscriber2 = SubscriberSocket::new(tcp::InitiatorTransport::new(tcp::StreamConnectionBuilder::new()));
//! 
//! publisher.bind(TransportMethod::Network(NetworkAddress::from_dns("localhost:12000".to_string()).unwrap()));
//! subscriber1.connect(TransportMethod::Network(NetworkAddress::from_dns("localhost:12000".to_string()).unwrap()));
//! subscriber2.connect(TransportMethod::Network(NetworkAddress::from_dns("localhost:12000".to_string()).unwrap()));
//! 
//! // Make sure that TCP connection is established otherwise the first sent message might not arrive 
//! std::thread::sleep(std::time::Duration::from_millis(100));
//! 
//! let payload: Vec<u8> = vec![2u8, 8u8];
//! 
//! publisher.send(RawMessage::new(payload.clone()), OpFlag::Wait);
//! let subscriber1_result = subscriber1.receive(OpFlag::Wait).unwrap().into_payload();
//! let subscriber2_result = subscriber2.receive(OpFlag::Wait).unwrap().into_payload();
//! 
//! assert_eq!(payload.clone(), subscriber1_result.clone());
//! assert_eq!(payload.clone(), subscriber2_result.clone());
//! # }
//! ```

use core::socket::{Socket, InwardSocket, OutwardSocket, SocketError, SocketInternalError, OpFlag, PeerIdentification};
use core::transport::{InitiatorTransport, AcceptorTransport, TransportMethod};
use core::message::{PeerId, RawMessage, MessageMetadata, Message};

use std::collections::{HashSet};

const PUBLISHER_MODELID: u16 = 0xFFE1; 

struct ConnectionTracker {
    peers: HashSet<PeerId>
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            peers: HashSet::new()
        }
    }

    pub fn accept_new_peer(&mut self, peer_id: PeerId) -> Result<(), SocketInternalError> {
        if self.peers.insert(peer_id) {
            Ok(())
        } else {
            Err(SocketInternalError::AlreadyConnected)
        }
    }

    pub fn check_peer_connected(&self, peer_id: PeerId) -> Result<(), (Option<PeerId>, SocketInternalError)> {
        if self.peers.contains(&peer_id) {
            Ok(())
        } else {
            Err((Some(peer_id), SocketInternalError::UnrelatedPeer))
        }
    }

    fn close_connection(&mut self, peer_id: PeerId) -> Result<(), SocketInternalError> {
        if self.peers.remove(&peer_id) {
            Ok(()) 
        } else {
            Err(SocketInternalError::UnknownPeer)
        }
    }
}

/// # Subscriber socket
/// This socket serves to connect to one or more publishers that distribute information among connected subscribers.
pub struct SubscriberSocket<T: InitiatorTransport> {
    transport: T,
    tracker: ConnectionTracker
}

impl<T> SubscriberSocket<T>
    where T: InitiatorTransport {
    /// Creates a new socket that will use the given underlieing transport to send and receive messages
    pub fn new(transport: T) -> Self {
        Self {
            transport: transport,
            tracker: ConnectionTracker::new()
        }
    }

    fn handle_received_message_model_id(&mut self, message: &RawMessage) -> Result<(), (Option<PeerId>, SocketError)> {
        if message.communication_model_id().unwrap() == PUBLISHER_MODELID {
            Ok(())
        } else {
            self.transport.close_connection(PeerIdentification::PeerId(message.peer_id().unwrap())).unwrap();
            Err((Some(message.peer_id().unwrap()), SocketError::IncompatiblePeer))
        }
    }
}

impl<T> Socket for SubscriberSocket<T>
    where T: InitiatorTransport {

    /// Connects to a subcriber
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        let peerid = self.transport.connect(target)?.expect("Transport did not provide peer id");
        self.tracker.accept_new_peer(peerid.clone())?;
        Ok(Some(peerid))
    }

    /// Will return error, since a subscriber socket cannot bind for listening
    fn bind(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        Err(SocketError::NotSupportedOperation)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), SocketError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.tracker.close_connection(peer_id)?;
                self.transport.close_connection(peer_identification).expect("Connection existance already checked, should not happen");
                Ok(())
            },
            PeerIdentification::TransportMethod(method) => {
                let peer_id = (self.transport.close_connection(PeerIdentification::TransportMethod(method))?).unwrap();
                self.tracker.close_connection(peer_id).expect("Connection existance already checked, should not happen");
                Ok(())
            }
        }
    }

    fn close(self) -> Result<(), SocketError> {
        self.transport.close().into()
    }
}

impl<T> InwardSocket for SubscriberSocket<T>
    where T: InitiatorTransport {

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        match self.transport.receive(flags) {
            Ok(message) => {
                self.handle_received_message_model_id(&message)?;
                self.tracker.check_peer_connected(message.peer_id().ok_or((None, SocketError::UnknownPeer))?).map_err(|(peerid, err)| {(peerid, SocketError::from(err))})?;
                Ok(message)
            }
            Err(err) => Err(err)
        }
    }
}

/// # Publisher socket
/// This socket can be used to distribute information to subscribing peers
pub struct PublisherSocket<T: AcceptorTransport> {
    transport: T
}

impl<T> PublisherSocket<T>
    where T: AcceptorTransport {
    /// Creates new socket with the underlieing transport to distribute messages to subscribint peers
    pub fn new(transport: T) -> Self {
        Self {
            transport: transport
        }
    }
}

impl<T> Socket for PublisherSocket<T>
    where T: AcceptorTransport {

    fn connect(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        Err(SocketError::NotSupportedOperation)
    }

    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        self.transport.bind(target)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), SocketError> {
        match peer_identification {
            PeerIdentification::PeerId(_peer_id) => {
                self.transport.close_connection(peer_identification).expect("Connection existance already checked, should not happen");
                Ok(())
            },
            PeerIdentification::TransportMethod(method) => {
                self.transport.close_connection(PeerIdentification::TransportMethod(method))?.unwrap();
                Ok(())
            }
        }
    }

    fn close(self) -> Result<(), SocketError> {
        self.transport.close()
    }
}

impl<T> OutwardSocket for PublisherSocket<T>
    where T: AcceptorTransport {

    fn send(&mut self, message:RawMessage, flags: OpFlag) -> Result<MessageMetadata, SocketError> {
        let processed_message = message.commit_communication_model_id(PUBLISHER_MODELID);
        let message_metadata = processed_message.metadata().clone();
        for peer_id in self.transport.query_connected_peers().iter() {
            self.transport.send(processed_message.clone().apply_peer_id(peer_id.clone()), flags.clone())?;
        };
        Ok(message_metadata)
    }
}

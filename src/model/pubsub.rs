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
//! let payload: Vec<u8> = vec![2u8, 8u8];
//! publisher.send(RawMessage::new(payload.clone()), OpFlag::Default);
//! let subscriber1_result = subscriber1.receive(OpFlag::Default).unwrap().into_payload();
//! let subscriber2_result = subscriber2.receive(OpFlag::Default).unwrap().into_payload();
//! 
//! assert_eq!(payload.clone(), subscriber1_result.clone());
//! assert_eq!(payload.clone(), subscriber2_result.clone());
//! # }
//! ```

use core::socket::{Socket, InwardSocket, OutwardSocket, SocketError, SocketInternalError, OpFlag, PeerIdentification};
use core::transport::{InitiatorTransport, AcceptorTransport, TransportMethod};
use core::message::{PeerId, RawMessage, MessageMetadata, Message};

use std::collections::{HashSet};

const SUBSCRIBER_MODELID: u16 = 0xFFE0;
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

pub struct SubscriberSocket<T: InitiatorTransport> {
    channel: T,
    tracker: ConnectionTracker
}

impl<T> SubscriberSocket<T>
    where T: InitiatorTransport {
    pub fn new(transport: T) -> Self {
        Self {
            channel: transport,
            tracker: ConnectionTracker::new()
        }
    }

    pub fn handle_received_message_model_id(&mut self, message: &RawMessage) -> Result<(), (Option<PeerId>, SocketError)> {
        if message.communication_model_id().unwrap() == PUBLISHER_MODELID {
            Ok(())
        } else {
            self.channel.close_connection(PeerIdentification::PeerId(message.peer_id().unwrap())).unwrap();
            Err((Some(message.peer_id().unwrap()), SocketError::IncompatiblePeer))
        }
    }
}

impl<T> Socket for SubscriberSocket<T>
    where T: InitiatorTransport {

    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        let peerid = self.channel.connect(target)?.expect("Transport did not provide peer id");
        self.tracker.accept_new_peer(peerid.clone())?;
        Ok(Some(peerid))
    }

    fn bind(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        Err(SocketError::NotSupportedOperation)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), SocketError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.tracker.close_connection(peer_id)?;
                self.channel.close_connection(peer_identification).expect("Connection existance already checked, should not happen");
                Ok(())
            },
            PeerIdentification::TransportMethod(method) => {
                let peer_id = (self.channel.close_connection(PeerIdentification::TransportMethod(method))?).unwrap();
                self.tracker.close_connection(peer_id).expect("onnection existance already checked, should not happen");
                Ok(())
            }
        }
    }

    fn close(self) -> Result<(), SocketError> {
        self.channel.close().into()
    }
}

impl<T> InwardSocket for SubscriberSocket<T>
    where T: InitiatorTransport {

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        match self.channel.receive(flags) {
            Ok(message) => {
                self.handle_received_message_model_id(&message)?;
                self.tracker.check_peer_connected(message.peer_id().ok_or((None, SocketError::UnknownPeer))?).map_err(|(peerid, err)| {(peerid, SocketError::from(err))})?;
                Ok(message)
            }
            Err(err) => Err(err)
        }
    }
}

pub struct PublisherSocket<T: AcceptorTransport> {
    channel: T
}

impl<T> PublisherSocket<T>
    where T: AcceptorTransport {
    pub fn new(transport: T) -> Self {
        Self {
            channel: transport
        }
    }

    pub fn handle_received_message_model_id(&mut self, message: &RawMessage) -> Result<(), (Option<PeerId>, SocketError)> {
        if message.communication_model_id().unwrap() == SUBSCRIBER_MODELID {
            Ok(())
        } else {
            self.channel.close_connection(PeerIdentification::PeerId(message.peer_id().unwrap())).unwrap();
            Err((Some(message.peer_id().unwrap()), SocketError::IncompatiblePeer))
        }
    }
}

impl<T> Socket for PublisherSocket<T>
    where T: AcceptorTransport {

    fn connect(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        Err(SocketError::NotSupportedOperation)
    }

    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        self.channel.bind(target)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), SocketError> {
        match peer_identification {
            PeerIdentification::PeerId(_peer_id) => {
                self.channel.close_connection(peer_identification).expect("Connection existance already checked, should not happen");
                Ok(())
            },
            PeerIdentification::TransportMethod(method) => {
                self.channel.close_connection(PeerIdentification::TransportMethod(method))?.unwrap();
                Ok(())
            }
        }
    }

    fn close(self) -> Result<(), SocketError> {
        self.channel.close()
    }
}

impl<T> OutwardSocket for PublisherSocket<T>
    where T: AcceptorTransport {

    fn send(&mut self, message:RawMessage, flags: OpFlag) -> Result<MessageMetadata, SocketError> {
        let processed_message = message.commit_communication_model_id(PUBLISHER_MODELID);
        let message_metadata = processed_message.metadata().clone();
        for peer_id in self.channel.query_connected_peers().iter() {
            self.channel.send(processed_message.clone().apply_peer_id(peer_id.clone()), flags.clone())?;
        };
        Ok(message_metadata)
    }
}

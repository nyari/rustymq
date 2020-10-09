use core::socket::{Socket, InwardSocket, OutwardSocket, ConnectorError, SocketError, OpFlag, PeerIdentification};
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

    pub fn accept_new_peer(&mut self, peer_id: PeerId) -> Result<(), ConnectorError> {
        if self.peers.insert(peer_id) {
            Ok(())
        } else {
            Err(ConnectorError::AlreadyConnected)
        }
    }

    pub fn check_peer_connected(&self, peer_id: PeerId) -> Result<(), (Option<PeerId>, SocketError)> {
        if self.peers.contains(&peer_id) {
            Ok(())
        } else {
            Err((Some(peer_id), SocketError::UnrelatedPeer))
        }
    }

    fn close_connection(&mut self, peer_id: PeerId) -> Result<(), ConnectorError> {
        if self.peers.remove(&peer_id) {
            Ok(()) 
        } else {
            Err(ConnectorError::UnknownPeer)
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

    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        let peerid = self.channel.connect(target)?.expect("Transport did not provide peer id");
        self.tracker.accept_new_peer(peerid.clone())?;
        Ok(Some(peerid))
    }

    fn bind(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        Err(ConnectorError::NotSupportedOperation)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), ConnectorError> {
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
        self.channel.close()
    }
}

impl<T> InwardSocket for SubscriberSocket<T>
    where T: InitiatorTransport {

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        match self.channel.receive(flags) {
            Ok(message) => {
                self.handle_received_message_model_id(&message)?;
                self.tracker.check_peer_connected(message.peer_id().ok_or((None, SocketError::UnknownPeer))?)?;
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

    fn connect(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        Err(ConnectorError::NotSupportedOperation)
    }

    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        self.channel.bind(target)
    }

    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), ConnectorError> {
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
        let processed_message = message.commit_conversation_model_id(PUBLISHER_MODELID);
        let message_metadata = processed_message.metadata().clone();
        for peer_id in self.channel.query_connected_peers().iter() {
            self.channel.send(processed_message.clone().apply_peer_id(peer_id.clone()), flags.clone())?;
        };
        Ok(message_metadata)
    }
}

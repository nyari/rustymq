use core::socket::{BidirectionalSocket, Socket, InwardSocket, OutwardSocket, ConnectorError, SocketError, OpFlag};
use core::transport::{InitiatorTransport, AcceptorTransport, TransportMethod};
use core::message::{PeerId, ConversationId, RawMessage, MessageMetadata, Message};

use std::collections::{HashMap, HashSet};

#[derive(Debug)]
enum ConnTrackerError {
    NotNewPeer
}

struct ConnectionTracker {
    map: HashMap<PeerId, HashSet<ConversationId>>
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            map: HashMap::new()
        }
    }

    pub fn accept_new_peer(&mut self, peer_id: PeerId) -> Result<(), ConnTrackerError> {
        match self.map.insert(peer_id, HashSet::new()) {
            None => Ok(()),
            _ => Err(ConnTrackerError::NotNewPeer)
        }
    }

    pub fn accept_peer(&mut self, peer_id: PeerId) {
        if !self.map.contains_key(&peer_id) {
            self.accept_new_peer(peer_id).expect("Internal error");
        }
    }

    pub fn get_single_peer(&self) -> Option<PeerId> {
        if self.map.len() == 1 {
            Some(self.map.keys().next().unwrap().clone())
        } else {
            None
        }
    }

    pub fn apply_single_peer_if_needed(&self, message: RawMessage) -> Result<RawMessage, SocketError> {
        match message.peer_id() {
            Some(_) => { Ok(message) }
            None => match self.get_single_peer() {
                Some(peer_id) => Ok(message.apply_peer_id(peer_id)),
                None => Err(SocketError::UnknownPeer)
            }
        }
    }

    pub fn initiate_new_conversation(&mut self, message: RawMessage) -> Result<RawMessage, SocketError> {
        match message.peer_id() {
            Some(peer_id) => match self.map.get_mut(peer_id) {
                Some(conversation_ids) => {
                    if conversation_ids.insert(message.conversation_id().clone()) {
                        Ok(message)
                    } else {
                        Err(SocketError::DuplicatedConversation)
                    }
                },
                None => Err(SocketError::UnrelatedPeer)
            },
            None => Err(SocketError::UnknownPeer)
        }
    }

    pub fn close_conversation(&mut self, message: RawMessage) -> Result<RawMessage, SocketError> {
        match message.peer_id() {
            Some(peer_id) => match self.map.get_mut(peer_id) {
                Some(conversation_ids) => {
                    if conversation_ids.remove(message.conversation_id()) {
                        Ok(message)
                    } else {
                        Err(SocketError::UnrelatedConversation)
                    }
                },
                None => Err(SocketError::UnrelatedPeer)
            },
            None => Err(SocketError::UnknownPeer)
        }
    }

    pub fn has_live_conversation(&self) -> bool{
        !self.map.is_empty()
    }
}

pub struct RequestSocket<T: InitiatorTransport> {
    channel: T,
    tracker: ConnectionTracker
}

impl<T> RequestSocket<T>
    where T: InitiatorTransport {
    pub fn new(transport: T) -> Self {
        Self {
            channel: transport,
            tracker: ConnectionTracker::new()
        }
    }
}

impl<T> Socket for RequestSocket<T>
    where T: InitiatorTransport {

    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        let peer_id = self.channel.connect(target)?.expect("Transport did not provide peer id");
        self.tracker.accept_peer(peer_id);
        Ok(Some(peer_id))
    }

    fn bind(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        Err(ConnectorError::NotSupportedOperation)
    }

    fn close(self) -> Result<(), SocketError> {
        self.channel.close()
    }
}

impl<T> InwardSocket for RequestSocket<T>
    where T: InitiatorTransport {

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, SocketError> {
        match self.channel.receive(flags) {
            Ok(message) => {
                self.tracker.close_conversation(message)
            }
            Err(err) => Err(err)
        }
    }
}

impl<T> OutwardSocket for RequestSocket<T>
    where T: InitiatorTransport
{

    fn send(&mut self, message:RawMessage, flags:OpFlag) -> Result<MessageMetadata, SocketError> {
        let metadata = message.metadata().clone();
        match self.channel.send(self.tracker.initiate_new_conversation(self.tracker.apply_single_peer_if_needed(message)?)?, flags) {
            Ok(()) => Ok(metadata),
            Err(err) => Err(err)
        }
    }
}

impl<T> BidirectionalSocket for RequestSocket<T>
    where T: InitiatorTransport
{
}

pub struct ReplySocket<T: AcceptorTransport> {
    channel: T,
    tracker: ConnectionTracker
}

impl<T> ReplySocket<T>
    where T: AcceptorTransport {
    pub fn new(transport: T) -> Self {
        Self {
            channel: transport,
            tracker: ConnectionTracker::new()
        }
    }
}

impl<T> Socket for ReplySocket<T>
    where T: AcceptorTransport {

    fn connect(&mut self, _target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        Err(ConnectorError::NotSupportedOperation)
    }

    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError> {
        self.channel.bind(target)
    }

    fn close(self) -> Result<(), SocketError> {
        self.channel.close()
    }
}

impl<T> InwardSocket for ReplySocket<T>
    where T: AcceptorTransport {

    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, SocketError> {
        let message = self.channel.receive(flags)?;
        self.tracker.accept_peer(message.peer_id().unwrap().clone());
        self.tracker.initiate_new_conversation(message)
    }
}

impl<T> OutwardSocket for ReplySocket<T>
    where T: AcceptorTransport {

    fn send(&mut self, message:RawMessage, flags: OpFlag) -> Result<MessageMetadata, SocketError> {
        let processed_message = self.tracker.close_conversation(message)?;
        let message_metadata = processed_message.metadata().clone();
        self.channel.send(processed_message, flags)?;
        Ok(message_metadata)
    }
}

impl<T> BidirectionalSocket for ReplySocket<T>
    where T: AcceptorTransport
{
}
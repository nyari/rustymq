//! # Request reply communication model
//! This module contains the socket implementation for a request-reply communication model.
//! ## Operation
//! The [`ReplySocket`] listens for incoming connections and shuld reply to all incoming messages.
//! The [`RequestSocket`] can be used to send requests to the reply sockets.
//! ## Example
//! ```rust
//! use rustymq::base::{Socket, InwardSocket, OutwardSocket, OpFlag, BidirectionalSocket,
//!                     Message, RawMessage,
//!                     TransportMethod, NetworkAddress};
//! use rustymq::model::{RequestSocket, ReplySocket};
//! use rustymq::transport::network::tcp;
//! # fn main() {
//!
//! let replier = ReplySocket::new(
//!                 tcp::AcceptorTransport::new(
//!                     tcp::StreamConnectionBuilder::new(),
//!                     tcp::StreamListenerBuilder::new()))
//!               .unwrap();
//! let requestor = RequestSocket::new(tcp::InitiatorTransport::new(tcp::StreamConnectionBuilder::new())).unwrap();
//!
//! replier.bind(NetworkAddress::from_dns("localhost:13000".to_string()).unwrap().into());
//! requestor.connect(NetworkAddress::from_dns("localhost:13000".to_string()).unwrap().into());
//!
//! // Make sure that TCP connection is established otherwise the first sent message might not arrive
//! std::thread::sleep(std::time::Duration::from_millis(100));
//!
//! let request_payload: Vec<u8> = vec![2u8, 8u8];
//! let response_payload_requirement: Vec<u8> = vec![2u8, 8u8, 16u8, 32u8];
//!
//! requestor.send(RawMessage::new(request_payload.clone()), OpFlag::Wait);
//! replier.respond(OpFlag::Wait, OpFlag::Wait, |message| {
//!     let mut payload = message.into_payload();
//!     let reply_payload_extend: Vec<u8> = vec![16u8, 32u8];
//!     payload.extend(reply_payload_extend.into_iter());
//!     RawMessage::new(payload)
//! });
//! let response_payload = requestor.receive(OpFlag::Wait).unwrap().into_payload();
//!
//! assert_eq!(response_payload.clone(), response_payload_requirement.clone());
//! # }
//! ```

use crate::base::config::TransportConfiguration;
use crate::base::message::{
    ConversationId, Message, MessageMetadata, Part, PartError, PeerId, RawMessage,
};
use crate::base::queue::{MessageQueueOverflowHandling, MessageQueueingPolicy};
use crate::base::socket::{
    BidirectionalSocket, InwardSocket, OpFlag, OutwardSocket, PeerIdentification, Socket,
    SocketError,
};
use crate::base::transport::{AcceptorTransport, InitiatorTransport, Transport, TransportMethod};
use crate::internals::socket::SocketInternalError;

use std::collections::HashMap;
use std::sync::Mutex;

const REQUEST_MODELID: u16 = 0xFFF0;
const REPLY_MODELID: u16 = 0xFFF1;

#[derive(Debug)]
enum ConnTrackerError {
    NotNewPeer,
}

#[derive(Debug, Clone)]
enum ConnTrackerState {
    RequestMessage(Part),
    ReplyMessage(Part),
}

struct ConnectionTracker {
    map: HashMap<PeerId, HashMap<ConversationId, ConnTrackerState>>,
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
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

    pub fn apply_single_peer_if_needed(
        &self,
        message: RawMessage,
    ) -> Result<RawMessage, SocketInternalError> {
        match message.peer_id() {
            Some(_) => Ok(message),
            None => match self.get_single_peer() {
                Some(peer_id) => Ok(message.apply_peer_id(peer_id)),
                None => Err(SocketInternalError::SocketUnknownPeer),
            },
        }
    }

    pub fn handle_request_message(
        &mut self,
        message: RawMessage,
    ) -> Result<RawMessage, SocketInternalError> {
        match message.peer_id() {
            Some(peer_id) => match self.map.get_mut(peer_id) {
                Some(conversation_map) => {
                    Self::handle_request_conversation_message(conversation_map, message)
                }
                None => Err(SocketInternalError::SocketUnrelatedPeer),
            },
            None => Err(SocketInternalError::SocketUnknownPeer),
        }
    }

    pub fn handle_reply_message(
        &mut self,
        message: RawMessage,
    ) -> Result<RawMessage, SocketInternalError> {
        match message.peer_id() {
            Some(peer_id) => match self.map.get_mut(peer_id) {
                Some(conversation_map) => {
                    Self::handle_reply_conversation_message(conversation_map, message)
                }
                None => Err(SocketInternalError::SocketUnrelatedPeer),
            },
            None => Err(SocketInternalError::SocketUnknownPeer),
        }
    }

    fn handle_request_conversation_message(
        conversation_map: &mut HashMap<ConversationId, ConnTrackerState>,
        message: RawMessage,
    ) -> Result<RawMessage, SocketInternalError> {
        match conversation_map.get_mut(
            &message
                .conversation_id()
                .ok_or(SocketInternalError::SocketConversationIdentifierMissing)?,
        ) {
            Some(state) => match state.clone() {
                ConnTrackerState::RequestMessage(part) if part.is_continueable() => {
                    if let ConnTrackerState::RequestMessage(part) = state {
                        part.update_to_next_part(message.part())
                            .map_err(|err| match err {
                                PartError::AlreadyFinishedMultipart => {
                                    SocketInternalError::SocketIncorrectStateError
                                }
                                _ => SocketInternalError::SocketUnrelatedConversationPart,
                            })?;
                    } else {
                        panic!("Internal error! Impossible case handled")
                    }
                    Ok(message)
                }
                _ => Err(SocketInternalError::SocketIncorrectStateError),
            },
            None => {
                if message.part().is_initial() {
                    conversation_map.insert(
                        message.conversation_id().unwrap().clone(),
                        ConnTrackerState::RequestMessage(message.part().clone()),
                    );
                    Ok(message)
                } else {
                    Err(SocketInternalError::SocketUnrelatedConversationPart)
                }
            }
        }
    }

    fn handle_reply_conversation_message(
        conversation_map: &mut HashMap<ConversationId, ConnTrackerState>,
        message: RawMessage,
    ) -> Result<RawMessage, SocketInternalError> {
        match conversation_map.get_mut(
            &message
                .conversation_id()
                .ok_or(SocketInternalError::SocketConversationIdentifierMissing)?,
        ) {
            Some(state) => match state.clone() {
                ConnTrackerState::RequestMessage(part) if part.is_last() => {
                    if message.part().is_initial() {
                        *state = ConnTrackerState::ReplyMessage(message.part().clone());
                        Ok(message)
                    } else {
                        Err(SocketInternalError::SocketUnrelatedConversationPart)
                    }
                }
                ConnTrackerState::ReplyMessage(part) if part.is_continueable() => {
                    if let ConnTrackerState::ReplyMessage(part) = state {
                        part.update_to_next_part(message.part())
                            .map_err(|err| match err {
                                PartError::AlreadyFinishedMultipart => {
                                    SocketInternalError::SocketIncorrectStateError
                                }
                                _ => SocketInternalError::SocketUnrelatedConversationPart,
                            })?;
                        if part.is_last() {
                            conversation_map
                                .remove(&message.conversation_id().unwrap())
                                .unwrap();
                        }
                    } else {
                        panic!("Internal error! Impossible case handled")
                    }
                    Ok(message)
                }
                _ => Err(SocketInternalError::SocketIncorrectStateError),
            },
            None => Err(SocketInternalError::SocketIncorrectStateError),
        }
    }

    fn accept_new_peer(&mut self, peer_id: PeerId) -> Result<(), ConnTrackerError> {
        match self.map.insert(peer_id, HashMap::new()) {
            None => Ok(()),
            _ => Err(ConnTrackerError::NotNewPeer),
        }
    }

    fn close_connection(&mut self, peer_id: PeerId) -> Result<(), SocketInternalError> {
        self.map
            .remove(&peer_id)
            .ok_or(SocketInternalError::SocketUnknownPeer)?;
        Ok(())
    }
}

fn validate_transport_configuration<T: Transport>(
    transport: &T,
) -> Result<(), TransportConfiguration> {
    match transport.query_configuration() {
        Some(config) => {
            match config.queue_policy.overflow {
                Some((MessageQueueOverflowHandling::Drop, depth)) => {
                    return Err(TransportConfiguration::new().with_queue_policy(
                        MessageQueueingPolicy::default()
                            .with_overflow(Some((MessageQueueOverflowHandling::Drop, depth))),
                    ))
                }
                _ => {}
            }
            Ok(())
        }
        None => Ok(()),
    }
}

/// # Request socket
/// This allows connections to peers running  [`ReplySocket`]s. It allowed to connect to multiple
/// reply sockets with the limitation that if multiple connections are managed then [`PeerId`]s have to be
/// specified in the [`MessageMetadata`] in order to know which connection to send the message to.
pub struct RequestSocket<T: InitiatorTransport> {
    transport: T,
    tracker: Mutex<ConnectionTracker>,
}

impl<T> RequestSocket<T>
where
    T: InitiatorTransport,
{
    /// Create a new request socket using the underlieing transport
    pub fn new(transport: T) -> Result<Self, TransportConfiguration> {
        validate_transport_configuration(&transport)?;
        Ok(Self {
            transport: transport,
            tracker: Mutex::new(ConnectionTracker::new()),
        })
    }

    fn handle_received_message_model_id(
        &self,
        message: &RawMessage,
    ) -> Result<(), (Option<PeerId>, SocketError)> {
        if message.communication_model_id().unwrap() == REPLY_MODELID {
            Ok(())
        } else {
            self.transport
                .close_connection(PeerIdentification::PeerId(message.peer_id().unwrap()))
                .unwrap();
            Err((
                Some(message.peer_id().unwrap()),
                SocketError::IncompatiblePeer,
            ))
        }
    }
}

impl<T> Socket for RequestSocket<T>
where
    T: InitiatorTransport,
{
    fn connect(&self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        let peer_id = self
            .transport
            .connect(target)?
            .expect("Transport did not provide peer id");
        self.tracker.lock().unwrap().accept_peer(peer_id);
        Ok(Some(peer_id))
    }

    fn bind(&self, _target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        Err(SocketError::NotSupportedOperation)
    }

    fn close_connection(&self, peer_identification: PeerIdentification) -> Result<(), SocketError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.tracker.lock().unwrap().close_connection(peer_id)?;
                self.transport
                    .close_connection(peer_identification)
                    .expect("Connection existance already checked, should not happen");
                Ok(())
            }
            PeerIdentification::TransportMethod(method) => {
                let peer_id = (self
                    .transport
                    .close_connection(PeerIdentification::TransportMethod(method))?)
                .unwrap();
                self.tracker
                    .lock()
                    .unwrap()
                    .close_connection(peer_id)
                    .expect("onnection existance already checked, should not happen");
                Ok(())
            }
        }
    }

    fn close(self) -> Result<(), SocketError> {
        self.transport.close()
    }
}

impl<T> InwardSocket for RequestSocket<T>
where
    T: InitiatorTransport,
{
    fn receive(&self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        match self.transport.receive(flags) {
            Ok(message) => {
                self.handle_received_message_model_id(&message)?;
                self.tracker
                    .lock()
                    .unwrap()
                    .handle_reply_message(message)
                    .map_err(|error| (None, SocketError::from(error)))
            }
            Err(err) => Err(err),
        }
    }
}

impl<T> OutwardSocket for RequestSocket<T>
where
    T: InitiatorTransport,
{
    fn send(&self, message: RawMessage, flags: OpFlag) -> Result<MessageMetadata, SocketError> {
        let request_message = message
            .apply_communication_model_id(REQUEST_MODELID)
            .ensure_random_conversation_id();
        let metadata = request_message.metadata().clone();
        let message = {
            let mut tracker = self.tracker.lock().unwrap();
            let message_with_peer_id = tracker.apply_single_peer_if_needed(request_message)?;
            tracker.handle_request_message(message_with_peer_id)
        }?;

        match self.transport.send(message, flags) {
            Ok(()) => Ok(metadata),
            Err(err) => Err(err),
        }
    }
}

impl<T> BidirectionalSocket for RequestSocket<T> where T: InitiatorTransport {}

/// # Reply socket
/// Allows for listening and responding to connections from [`RequestSocket`]s. It can handle multiple
/// incoming connections from several different clients
pub struct ReplySocket<T: AcceptorTransport> {
    transport: T,
    tracker: Mutex<ConnectionTracker>,
}

impl<T> ReplySocket<T>
where
    T: AcceptorTransport,
{
    pub fn new(transport: T) -> Result<Self, TransportConfiguration> {
        validate_transport_configuration(&transport)?;
        Ok(Self {
            transport: transport,
            tracker: Mutex::new(ConnectionTracker::new()),
        })
    }

    pub fn handle_received_message_model_id(
        &self,
        message: &RawMessage,
    ) -> Result<(), (Option<PeerId>, SocketError)> {
        if message.communication_model_id().unwrap() == REQUEST_MODELID {
            Ok(())
        } else {
            self.transport
                .close_connection(PeerIdentification::PeerId(message.peer_id().unwrap()))
                .unwrap();
            Err((
                Some(message.peer_id().unwrap()),
                SocketError::IncompatiblePeer,
            ))
        }
    }
}

impl<T> Socket for ReplySocket<T>
where
    T: AcceptorTransport,
{
    fn connect(&self, _target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        Err(SocketError::NotSupportedOperation)
    }

    fn bind(&self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        self.transport.bind(target)
    }

    fn close_connection(&self, peer_identification: PeerIdentification) -> Result<(), SocketError> {
        match peer_identification {
            PeerIdentification::PeerId(peer_id) => {
                self.tracker.lock().unwrap().close_connection(peer_id)?;
                self.transport
                    .close_connection(peer_identification)
                    .expect("Connection existance already checked, should not happen");
                Ok(())
            }
            PeerIdentification::TransportMethod(method) => {
                let peer_id = (self
                    .transport
                    .close_connection(PeerIdentification::TransportMethod(method))?)
                .unwrap();
                self.tracker
                    .lock()
                    .unwrap()
                    .close_connection(peer_id)
                    .expect("Connection existance already checked, should not happen");
                Ok(())
            }
        }
    }

    fn close(self) -> Result<(), SocketError> {
        self.transport.close()
    }
}

impl<T> InwardSocket for ReplySocket<T>
where
    T: AcceptorTransport,
{
    fn receive(&self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        let message = self.transport.receive(flags)?;
        self.handle_received_message_model_id(&message)?;
        let mut tracker = self.tracker.lock().unwrap();
        tracker.accept_peer(message.peer_id().unwrap().clone());
        tracker
            .handle_request_message(message)
            .map_err(|error| (None, SocketError::from(error)))
    }
}

impl<T> OutwardSocket for ReplySocket<T>
where
    T: AcceptorTransport,
{
    fn send(&self, message: RawMessage, flags: OpFlag) -> Result<MessageMetadata, SocketError> {
        let processed_message = self
            .tracker
            .lock()
            .unwrap()
            .handle_reply_message(message)?
            .apply_communication_model_id(REPLY_MODELID);
        let message_metadata = processed_message.metadata().clone();
        self.transport.send(processed_message, flags)?;
        Ok(message_metadata)
    }
}

impl<T> BidirectionalSocket for ReplySocket<T> where T: AcceptorTransport {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_tracker_single_peer_single_part_request_reply_correct() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let request = tracker
            .handle_request_message(
                RawMessage::new(vec![])
                    .apply_peer_id(peer)
                    .ensure_random_conversation_id(),
            )
            .unwrap();
        tracker.handle_reply_message(request).unwrap();
    }

    #[test]
    fn connection_tracker_single_peer_single_part_request_twice() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let request = tracker
            .handle_request_message(
                RawMessage::new(vec![])
                    .apply_peer_id(peer)
                    .ensure_random_conversation_id(),
            )
            .unwrap();
        assert!(std::matches!(
            tracker.handle_request_message(request),
            Err(SocketInternalError::SocketIncorrectStateError)
        ));
    }

    #[test]
    fn connection_tracker_single_peer_single_part_request_once_reply_twice() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let request = tracker
            .handle_request_message(
                RawMessage::new(vec![])
                    .apply_peer_id(peer)
                    .ensure_random_conversation_id(),
            )
            .unwrap();
        let reply = tracker.handle_reply_message(request).unwrap();
        assert!(std::matches!(
            tracker.handle_reply_message(reply),
            Err(SocketInternalError::SocketIncorrectStateError)
        ));
    }

    #[test]
    fn connection_tracker_single_peer_single_part_request_reply_multiple_conversation_correct() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let request1 = tracker
            .handle_request_message(
                RawMessage::new(vec![])
                    .apply_peer_id(peer)
                    .ensure_random_conversation_id(),
            )
            .unwrap();
        let request2 = tracker
            .handle_request_message(
                RawMessage::new(vec![])
                    .apply_peer_id(peer)
                    .ensure_random_conversation_id(),
            )
            .unwrap();
        tracker.handle_reply_message(request2).unwrap();
        tracker.handle_reply_message(request1).unwrap();
    }

    #[test]
    fn connection_tracker_single_peer_multipart_request_single_response_correct() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let metadata = MessageMetadata::new_multipart()
            .apply_peer_id(peer)
            .ensure_random_conversation_id();
        let message_part1 = RawMessage::with_metadata(metadata.clone(), vec![]);
        let message_part2 =
            RawMessage::with_metadata(metadata.clone().next_multipart().unwrap(), vec![]);
        let message_part3 = RawMessage::with_metadata(
            metadata
                .clone()
                .next_multipart()
                .unwrap()
                .next_final_multipart()
                .unwrap(),
            vec![],
        );

        tracker.handle_request_message(message_part1).unwrap();
        tracker.handle_request_message(message_part2).unwrap();
        tracker.handle_request_message(message_part3).unwrap();

        let reply = tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata.continue_conversation(),
                vec![],
            ))
            .unwrap();
        assert!(std::matches!(
            tracker.handle_reply_message(reply),
            Err(SocketInternalError::SocketIncorrectStateError)
        ));
    }

    #[test]
    fn connection_tracker_single_peer_multipart_request_single_response_no_final_multipart_message_before_reply(
    ) {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let metadata = MessageMetadata::new_multipart()
            .apply_peer_id(peer)
            .ensure_random_conversation_id();
        let message_part1 = RawMessage::with_metadata(metadata.clone(), vec![]);
        let message_part2 =
            RawMessage::with_metadata(metadata.clone().next_multipart().unwrap(), vec![]);
        let message_part3 = RawMessage::with_metadata(
            metadata
                .clone()
                .next_multipart()
                .unwrap()
                .next_multipart()
                .unwrap(),
            vec![],
        );

        tracker.handle_request_message(message_part1).unwrap();
        tracker.handle_request_message(message_part2).unwrap();
        tracker.handle_request_message(message_part3).unwrap();

        assert!(std::matches!(
            tracker.handle_reply_message(RawMessage::with_metadata(
                metadata.continue_conversation(),
                vec![]
            )),
            Err(SocketInternalError::SocketIncorrectStateError)
        ));
    }

    #[test]
    fn connection_tracker_single_peer_single_request_multipart_response_correct() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let metadata = MessageMetadata::new()
            .apply_peer_id(peer)
            .ensure_random_conversation_id();
        let message = RawMessage::with_metadata(metadata.clone(), vec![]);

        tracker.handle_request_message(message).unwrap();

        tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata.clone().continue_conversation().started_multipart(),
                vec![],
            ))
            .unwrap();
        tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata
                    .clone()
                    .continue_conversation()
                    .started_multipart()
                    .next_multipart()
                    .unwrap(),
                vec![],
            ))
            .unwrap();
        let reply = tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata
                    .clone()
                    .continue_conversation()
                    .started_multipart()
                    .next_multipart()
                    .unwrap()
                    .next_final_multipart()
                    .unwrap(),
                vec![],
            ))
            .unwrap();
        assert!(std::matches!(
            tracker.handle_reply_message(reply),
            Err(SocketInternalError::SocketIncorrectStateError)
        ));
    }

    #[test]
    fn connection_tracker_single_peer_multipart_request_multipart_response_correct() {
        let mut tracker = ConnectionTracker::new();
        let peer = PeerId::new(1);
        tracker.accept_peer(peer);
        let metadata = MessageMetadata::new_multipart()
            .apply_peer_id(peer)
            .ensure_random_conversation_id();
        let message_part1 = RawMessage::with_metadata(metadata.clone(), vec![]);
        let message_part2 =
            RawMessage::with_metadata(metadata.clone().next_multipart().unwrap(), vec![]);
        let message_part3 = RawMessage::with_metadata(
            metadata
                .clone()
                .next_multipart()
                .unwrap()
                .next_final_multipart()
                .unwrap(),
            vec![],
        );

        tracker.handle_request_message(message_part1).unwrap();
        tracker.handle_request_message(message_part2).unwrap();
        tracker.handle_request_message(message_part3).unwrap();

        tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata.clone().continue_conversation().started_multipart(),
                vec![],
            ))
            .unwrap();
        tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata
                    .clone()
                    .continue_conversation()
                    .started_multipart()
                    .next_multipart()
                    .unwrap(),
                vec![],
            ))
            .unwrap();
        let reply = tracker
            .handle_reply_message(RawMessage::with_metadata(
                metadata
                    .clone()
                    .continue_conversation()
                    .started_multipart()
                    .next_multipart()
                    .unwrap()
                    .next_final_multipart()
                    .unwrap(),
                vec![],
            ))
            .unwrap();
        assert!(std::matches!(
            tracker.handle_reply_message(reply),
            Err(SocketInternalError::SocketIncorrectStateError)
        ));
    }
}

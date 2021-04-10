//! # Socket module
//! This module contains the traits serving as the main interaction point for RustyMQ

use core::message::{
    Message, MessageMetadata, PeerId, RawMessage, SerializableMessagePayload, TypedMessage,
};
use core::transport::TransportMethod;

use std::convert::{From, TryFrom};
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

/// # Operation flags
/// Configuration for individual send and receive calls on [`InwardSocket`]s and [`OutwardSocket`]s
#[derive(Debug, Clone)]
pub enum OpFlag {
    /// Wait for operation to finish
    Wait,
    /// Do not wait for operation to finish. Asychronous mode
    NoWait,
}

/// # [`Socket`] errors
/// Contains the possible errors for a socket that may need to be handled on the user side
#[derive(Debug, Clone)]
pub enum SocketError {
    /// Signals if the socket is not in the correct state to process the requested operation
    IncorrectStateError,
    /// The specified peer identifier in [`MessageMetadata`] is unkown by the
    UnrelatedPeer,
    /// The specified conversation in the [`MessageMetadata`] is not known by the socket
    UnrelatedConversation,
    /// The specified conversation part in the [`MessageMetadata`] is not known by the socket
    UnrelatedConversationPart,
    /// The internal gueue in [`Socket`] or [`Transport`] has reached its limit
    QueueDepthReached,
    /// The target peer identifier is not specified in the [`MessageMetadata`] and cannot be inferred
    UnknownPeer,
    /// The specific [`TransportMethod`] is already used by the socket
    TransportMethodAlreadyInUse,
    /// The specific [`TransportMethod`] cannot be found, cannot be connected to
    TransportTargetUnreachable,
    /// The tharget peer specified in [`MessageMetadata`] disconnected
    Disconnected,
    /// The tharget specified in [`TransportMethod`] refused the connection
    ConnectionRefused,
    /// The operation timed out
    Timeout,
    /// The connected peer has an incompatible version or communication model
    IncompatiblePeer,
    /// Handshake with the target peer failed
    HandshakeFailed,
    /// The [`OpFlag`] specified for the [`InwardSocket`] or [`OutwardSocket`] operation is not supprted by
    /// the socket or the underlieing Transport
    UnsupportedOpFlag(OpFlag),
    /// DNS domain name required but not available in NetworkAddress
    MissingDNSDomain,
    /// The [`TransportMethod`] is not supported by the Transport
    InvalidTransportMethod,
    /// The connection already exists
    AlreadyConnected,
    /// The requested operation is not supported by socket
    NotSupportedOperation,
    /// Could not connect to address provided by [`TransportMethod`]
    CouldNotConnect,
}

/// # [`Socket`] errors
/// Contains the possible errors for a socket that may need to be handled internally or by the user
#[derive(Debug, Clone)]
pub enum SocketInternalError {
    IncorrectStateError,
    UnrelatedPeer,
    UnrelatedConversation,
    UnrelatedConversationPart,
    QueueDepthReached,
    UnknownPeer,
    TransportMethodAlreadyInUse,
    TransportTargetUnreachable,
    Disconnected,
    ConnectionRefused,
    Timeout,
    IncompatiblePeer,
    HandshakeFailed,
    UnsupportedOpFlag(OpFlag),
    MissingDNSDomain,
    InvalidTransportMethod,
    AlreadyConnected,
    NotSupportedOperation,
    CouldNotConnect,

    IncompleteData,
    UnknownInternalError,
    UnknownDataFormatReceived,
}

impl SocketInternalError {
    /// Create user [`SocketError`] from internal error. Will panic in case of error is not handleable by user
    pub fn externalize_result<T>(result: Result<T, SocketInternalError>) -> Result<T, SocketError> {
        result.map_err(|x| SocketError::from(x))
    }

    /// Create user [`SocketError`] from internal error. Will panic in case of error is not handleable by user
    pub fn externalize_error(err: SocketInternalError) -> SocketError {
        SocketError::from(err)
    }
}

impl From<SocketInternalError> for SocketError {
    /// Convert [`SocketInternalError`] to [`SocketError`] . Will panic in case of error is not handleable by user
    fn from(input: SocketInternalError) -> SocketError {
        match input {
            SocketInternalError::IncorrectStateError => SocketError::IncorrectStateError,
            SocketInternalError::UnrelatedPeer => SocketError::UnrelatedPeer,
            SocketInternalError::UnrelatedConversation => SocketError::UnrelatedConversation,
            SocketInternalError::UnrelatedConversationPart => {
                SocketError::UnrelatedConversationPart
            }
            SocketInternalError::QueueDepthReached => SocketError::QueueDepthReached,
            SocketInternalError::UnknownPeer => SocketError::UnknownPeer,
            SocketInternalError::TransportMethodAlreadyInUse => {
                SocketError::TransportMethodAlreadyInUse
            }
            SocketInternalError::TransportTargetUnreachable => {
                SocketError::TransportTargetUnreachable
            }
            SocketInternalError::Disconnected => SocketError::Disconnected,
            SocketInternalError::ConnectionRefused => SocketError::ConnectionRefused,
            SocketInternalError::Timeout => SocketError::Timeout,
            SocketInternalError::IncompatiblePeer => SocketError::IncompatiblePeer,
            SocketInternalError::HandshakeFailed => SocketError::HandshakeFailed,
            SocketInternalError::UnsupportedOpFlag(op_flag) => {
                SocketError::UnsupportedOpFlag(op_flag)
            }
            SocketInternalError::MissingDNSDomain => SocketError::MissingDNSDomain,
            SocketInternalError::InvalidTransportMethod => SocketError::InvalidTransportMethod,
            SocketInternalError::AlreadyConnected => SocketError::AlreadyConnected,
            SocketInternalError::NotSupportedOperation => SocketError::NotSupportedOperation,
            SocketInternalError::CouldNotConnect => SocketError::CouldNotConnect,
            err => panic!(
                "SocketInternalError occured that cannot be converted to SocketError: {:?}",
                err
            ),
        }
    }
}

/// Identifier for a peer
pub enum PeerIdentification {
    /// Identify by the peer identifier in [`MessageMetadata`]
    PeerId(PeerId),
    /// Identify by the original [`TransportMethod`]
    TransportMethod(TransportMethod),
}

/// Result type for a typed sending operation
#[derive(Debug)]
pub enum SendTypedError<T>
where
    T: SerializableMessagePayload,
{
    /// Issue was related to the socket during the sending operation
    Socket(SocketError),
    /// Issue happened during the serialization of the typed value
    Conversion(<RawMessage as TryFrom<TypedMessage<T>>>::Error),
}

/// Result type for a typed receive operation
#[derive(Debug)]
pub enum ReceiveTypedError<T>
where
    T: SerializableMessagePayload,
{
    /// Issue was related to the socked during the receiving operation
    Socket((Option<PeerId>, SocketError)),
    /// Issue happened during the deserialization of the received typed value
    Conversion(<TypedMessage<T> as TryFrom<RawMessage>>::Error),
}

/// Result type for a typed query operation (a send and receive operation in some order consecutively)
#[derive(Debug)]
pub enum QueryTypedError<SEND, RECIEVE>
where
    SEND: SerializableMessagePayload,
    RECIEVE: SerializableMessagePayload,
{
    /// Issue happened during the sending operation. For details please see [`SendTypedError`]
    Send(SendTypedError<SEND>),
    /// Issue happened during the receiveing operation. For details please see [`ReceiveTypedError`]
    Receive(ReceiveTypedError<RECIEVE>),
}

/// # General **Socket** trait
/// The main interface for the user to interact with during operaton
/// There can be several implementations of it dependinng on communication model
pub trait Socket: Send + Sync {
    /// Connect to a new peer specified by the parameter. The result can contain the PeerId which should be used
    /// in [`MessageMetadata`] in future transactions to identify the peer
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError>;
    /// Bind to an interface to listen for connections given by the first parameter. The result can contain a PeerId that will contain
    /// the self identifier PeerId.
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError>;
    /// Close connection to a peer specified by [`PeerIdentification`]
    fn close_connection(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<(), SocketError>;
    /// Close the socket with all its connections
    fn close(self) -> Result<(), SocketError>;
}

/// # Outward **Socket** trait
/// The main interface for the user to send messages through the opened RustyMQ socket
/// There can be several implementations of it depending on the communication model used
pub trait OutwardSocket: Socket {
    /// Send a raw message
    fn send(&mut self, message: RawMessage, flags: OpFlag) -> Result<MessageMetadata, SocketError>;

    /// Send a typed message. Requires T to implement the SerializableMessagePayload trait
    fn send_typed<T>(
        &mut self,
        message: TypedMessage<T>,
        flags: OpFlag,
    ) -> Result<MessageMetadata, SendTypedError<T>>
    where
        T: SerializableMessagePayload,
    {
        match RawMessage::try_from(message) {
            Ok(msg) => match self.send(msg, flags) {
                Ok(metadata) => Ok(metadata),
                Err(err) => Err(SendTypedError::Socket(err)),
            },
            Err(err) => Err(SendTypedError::Conversion(err)),
        }
    }
}

/// # Inward **Socket** trait
/// The main interface for the user to receive messages through the opened RustyMQ socket
/// There can be several implementations of it depending on the communication model used
pub trait InwardSocket: Socket {
    /// Receive a raw message
    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)>;

    /// Receive a typed message. Requires T to implement the SerializableMessagePayload trait
    fn receive_typed<T>(&mut self, flags: OpFlag) -> Result<TypedMessage<T>, ReceiveTypedError<T>>
    where
        T: SerializableMessagePayload,
    {
        match self.receive(flags) {
            Ok(msg) => match TypedMessage::try_from(msg) {
                Ok(message) => Ok(message),
                Err(err) => Err(ReceiveTypedError::Conversion(err)),
            },
            Err(err) => Err(ReceiveTypedError::Socket(err)),
        }
    }
}

/// # Bidirectional **Socket** trait
/// The main interface for the user to send and receive messages through the same opened RustyMQ socket
/// There can be several implementations of it depending on the communication model used
pub trait BidirectionalSocket: OutwardSocket + InwardSocket {
    /// Execute a query (send and then receive) with a raw message
    /// This does not guaratee that the response is for the same message that was sent
    fn query(
        &mut self,
        message: RawMessage,
        send_flags: OpFlag,
        recieve_flags: OpFlag,
    ) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.send(message, send_flags)
            .map_err(|error| (None, error))?;
        self.receive(recieve_flags)
    }

    /// Execute a query (send and then receive) with a typed message. Requires S and R to be convertibe to implement [`SerializableMessagePayload`]
    fn query_typed<S, R>(
        &mut self,
        message: TypedMessage<S>,
        send_flags: OpFlag,
        recieve_flags: OpFlag,
    ) -> Result<TypedMessage<R>, QueryTypedError<S, R>>
    where
        S: SerializableMessagePayload,
        R: SerializableMessagePayload,
    {
        match self.send_typed(message, send_flags) {
            Ok(_) => match self.receive_typed(recieve_flags) {
                Ok(msg) => Ok(msg),
                Err(err) => Err(QueryTypedError::Receive(err)),
            },
            Err(err) => Err(QueryTypedError::Send(err)),
        }
    }

    /// Respont to a query (receive and then reply) with a raw message
    fn respond<T: Fn(RawMessage) -> RawMessage>(
        &mut self,
        receive_flags: OpFlag,
        send_flags: OpFlag,
        processor: T,
    ) -> Result<(), (Option<PeerId>, SocketError)> {
        let query = self.receive(receive_flags)?;
        let query_metadata = query.metadata().clone();
        let response = processor(query).continue_exchange_metadata(query_metadata);
        self.send(response, send_flags)
            .map_err(|error| (None, error))?;
        Ok(())
    }

    /// Respond to a query (receive and then reply) with a typed message. Requires S and R to be convertibe to implement [`SerializableMessagePayload`]
    fn respond_typed<R, S, Q: Fn(TypedMessage<R>) -> TypedMessage<S>>(
        &mut self,
        receive_flags: OpFlag,
        send_flags: OpFlag,
        processor: Q,
    ) -> Result<(), QueryTypedError<S, R>>
    where
        R: SerializableMessagePayload,
        S: SerializableMessagePayload,
    {
        match self.receive_typed(receive_flags) {
            Ok(msg) => {
                let response = processor(msg);
                match self.send_typed(response, send_flags) {
                    Ok(_) => Ok(()),
                    Err(err) => Err(QueryTypedError::Send(err)),
                }
            }
            Err(err) => Err(QueryTypedError::Receive(err)),
        }
    }
}

/// # ArcSocket
/// Wrapper around a [`Socket`] that allows for easier sharing between threads
pub struct ArcSocket<T>
where
    T: Socket,
{
    socket: Arc<Mutex<T>>,
}

impl<T> Clone for ArcSocket<T>
where
    T: Socket,
{
    fn clone(&self) -> Self {
        Self {
            socket: self.socket.clone(),
        }
    }
}

impl<T> ArcSocket<T>
where
    T: Socket,
{
    /// Create a new ArcSocket with an already constructed socket
    pub fn new(socket: T) -> Self {
        Self {
            socket: Arc::new(Mutex::new(socket)),
        }
    }

    /// Get a mutex guard for the internal socket
    pub fn lock_ref<'a>(&'a self) -> MutexGuard<'a, T> {
        self.socket.lock().unwrap()
    }

    /// Perform operations on internal socket directly through a closure
    pub fn direct<U, F: Fn(&T) -> U>(&self, func: F) -> U {
        func(self.socket.lock().unwrap().deref())
    }

    /// Perform mutating operations on internal socket directly through a closure
    pub fn direct_mut<U, F: FnMut(&T) -> U>(&mut self, mut func: F) -> U {
        func(self.socket.lock().unwrap().deref())
    }
}

impl<T> Socket for ArcSocket<T>
where
    T: Socket,
{
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        self.lock_ref().connect(target)
    }

    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, SocketError> {
        self.lock_ref().bind(target)
    }

    fn close_connection(
        &mut self,
        peer_identification: PeerIdentification,
    ) -> Result<(), SocketError> {
        self.lock_ref().close_connection(peer_identification)
    }

    fn close(self) -> Result<(), SocketError> {
        Ok(())
    }
}

impl<T> InwardSocket for ArcSocket<T>
where
    T: InwardSocket,
{
    fn receive(&mut self, flags: OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.lock_ref().receive(flags)
    }
}

impl<T> OutwardSocket for ArcSocket<T>
where
    T: OutwardSocket,
{
    fn send(
        &mut self,
        message: RawMessage,
        _flags: OpFlag,
    ) -> Result<MessageMetadata, SocketError> {
        self.lock_ref().send(message, OpFlag::NoWait)
    }
}

impl<T> BidirectionalSocket for ArcSocket<T> where T: BidirectionalSocket {}

use core::message::{Message, RawMessage, TypedMessage, PeerId, MessageMetadata, Buffer};
use core::transport::{TransportMethod};

use std::convert::{TryFrom, TryInto};
use std::time::{Duration};

#[derive(Debug)]
#[derive(Clone)]
pub enum OpFlag {
    Default,
    NoWait,
    Timeout(Duration)
}

#[derive(Debug)]
#[derive(Clone)]
pub enum SocketError {
    IncorrectStateError,
    InternalError,
    DuplicatedConversation,
    UnrelatedPeer,
    UnrelatedConversation,
    UnknownPeer,
    TransportMethodAlreadyInUse,
    TransportTargetUnreachable,
    Disconnected,
    ConnectionRefused,
    IncompleteData,
    UnknownDataFormatReceived,
    Timeout,
    IncompatiblePeer,
    HandshakeFailed,
    UnsupportedOpFlag(OpFlag),
    MissingDNSDomain
}

#[derive(Debug)]
#[derive(Clone)]
pub enum ConnectorError {
    InvalidTransportMethod,
    AlreadyConnected,
    AlreadyInUse,
    NotSupportedOperation,
    CouldNotConnect,
    InternalError,
    AlreadyDisconnected,
    UnknownPeer
}

pub enum PeerIdentification {
    PeerId(PeerId),
    TransportMethod(TransportMethod)
}

#[derive(Debug)]
pub enum SendTypedError<T> where T: TryInto<Buffer>, Buffer: TryInto<T>, <T as TryInto<Buffer>>::Error : std::fmt::Debug {
    Socket(SocketError),
    Conversion(<RawMessage as TryFrom<TypedMessage<T>>>::Error)
}

#[derive(Debug)]
pub enum ReceiveTypedError<T> where T: TryInto<Buffer>, Buffer: TryInto<T>, <Buffer as TryInto<T>>::Error : std::fmt::Debug {
    Socket((Option<PeerId>, SocketError)),
    Conversion(<TypedMessage<T> as TryFrom<RawMessage>>::Error)
}

#[derive(Debug)]
pub enum QueryTypedError<T> 
    where T: TryInto<Buffer>,
          Buffer: TryInto<T>,
          <T as TryInto<Buffer>>::Error : std::fmt::Debug,
          <Buffer as TryInto<T>>::Error : std::fmt::Debug
{
    Send(SendTypedError<T>),
    Receive(ReceiveTypedError<T>)
}

pub trait Socket : Send + Sync {
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError>;
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError>;
    fn close_connection(&mut self, peer_identification: PeerIdentification) -> Result<(), ConnectorError>;
    fn close(self) -> Result<(), SocketError>;
}
 
pub trait OutwardSocket : Socket {
    fn send(&mut self, message:RawMessage, flags:OpFlag) -> Result<MessageMetadata, SocketError>;

    fn send_typed<T>(&mut self, message:TypedMessage<T>, flags: OpFlag) -> Result<MessageMetadata, SendTypedError<T>>
        where T:TryInto<Buffer> + TryFrom<Buffer>,
              <T as TryInto<Buffer>>::Error: std::fmt::Debug 
    {
        match RawMessage::try_from(message) {
            Ok(msg) => match self.send(msg, flags) {
                Ok(metadata) => Ok(metadata),
                Err(err) => Err(SendTypedError::Socket(err))
            },
            Err(err) => Err(SendTypedError::Conversion(err))
        }
    }
}

pub trait InwardSocket : Socket {
    fn receive(&mut self, flags:OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)>;

    fn receive_typed<T>(&mut self, flags:OpFlag) -> Result<TypedMessage<T>, ReceiveTypedError<T>>
        where T:TryInto<Buffer> + TryFrom<Buffer>,
              <T as TryFrom<Buffer>>::Error: std::fmt::Debug 
    {
        match self.receive(flags) {
            Ok(msg) => match TypedMessage::try_from(msg) {
                Ok(message) => Ok(message),
                Err(err) => Err(ReceiveTypedError::Conversion(err))
            },
            Err(err) => Err(ReceiveTypedError::Socket(err))
        }
    }
}

pub trait BidirectionalSocket: OutwardSocket + InwardSocket
{
    fn query(&mut self, message :RawMessage, flags:OpFlag) -> Result<RawMessage, (Option<PeerId>, SocketError)> {
        self.send(message, flags.clone()).map_err(|error| {(None, error)})?;
        self.receive(flags)
    }

    fn query_typed<T>(&mut self, message:TypedMessage<T>, flags:OpFlag) -> Result<TypedMessage<T>, QueryTypedError<T>>
        where T:TryInto<Buffer> + TryFrom<Buffer>,
              <T as TryInto<Buffer>>::Error: std::fmt::Debug,
              <T as TryFrom<Buffer>>::Error: std::fmt::Debug 
    {
        match self.send_typed(message, flags.clone()) {
            Ok(_) => match self.receive_typed(flags) {
                Ok(msg) => Ok(msg),
                Err(err) => Err(QueryTypedError::Receive(err))
            },
            Err(err) => Err(QueryTypedError::Send(err))
        }
    }

    fn respond<T: Fn(RawMessage) -> RawMessage> (&mut self, flags:OpFlag, processor: T) -> Result<(), (Option<PeerId>, SocketError)> {
        let query = self.receive(flags.clone())?;
        let query_metadata = query.metadata().clone();
        let response = processor(query).continue_exchange_metadata(query_metadata);
        self.send(response, flags).map_err(|error| {(None, error)})?;
        Ok(())
    }

    fn respond_typed<T, Q: Fn(TypedMessage<T>) -> TypedMessage<T>>(&mut self, flags:OpFlag, processor: Q) -> Result<(), QueryTypedError<T>>
        where T:TryInto<Buffer> + TryFrom<Buffer>,
              <T as TryInto<Buffer>>::Error: std::fmt::Debug,
              <T as TryFrom<Buffer>>::Error: std::fmt::Debug 

    {
        match self.receive_typed(flags.clone()) {
            Ok(msg) => {
                let response = processor(msg);
                match self.send_typed(response, flags) {
                    Ok(_) => Ok(()),
                    Err(err) => Err(QueryTypedError::Send(err))
                }
            },
            Err(err) => Err(QueryTypedError::Receive(err))
        }
    }
}
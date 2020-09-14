use core::message::{Message, RawMessage, TypedMessage, TryIntoFromBuffer, PeerId, MessageMetadata};
use core::transport::{TransportMethod};

use std::convert::{TryFrom};
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
    UnsupportedOpFlag(OpFlag)
}

#[derive(Debug)]
#[derive(Clone)]
pub enum ConnectorError {
    InvalidTransportMethod,
    AlreadyConnected,
    AlreadyInUse,
    NotSupportedOperation,
    CouldNotConnect,
    InternalError
}

#[derive(Debug)]
pub enum SendTypedError<T:TryIntoFromBuffer> {
    Socket(SocketError),
    Conversion(<RawMessage as TryFrom<TypedMessage<T>>>::Error)
}

#[derive(Debug)]
pub enum ReceiveTypedError<T:TryIntoFromBuffer> {
    Socket(SocketError),
    Conversion(<TypedMessage<T> as TryFrom<RawMessage>>::Error)
}

#[derive(Debug)]
pub enum QueryTypedError<T:TryIntoFromBuffer> {
    Send(SendTypedError<T>),
    Receive(ReceiveTypedError<T>)
}

pub trait Socket : Send + Sync {
    fn connect(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError>;
    fn bind(&mut self, target: TransportMethod) -> Result<Option<PeerId>, ConnectorError>;
    fn close(self) -> Result<(), SocketError>;
}
 
pub trait OutwardSocket : Socket {
    fn send(&mut self, message:RawMessage, flags:OpFlag) -> Result<MessageMetadata, SocketError>;

    fn send_typed<T:TryIntoFromBuffer>(&mut self, message:TypedMessage<T>, flags: OpFlag) -> Result<MessageMetadata, SendTypedError<T>> {
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
    fn receive(&mut self, flags:OpFlag) -> Result<RawMessage, SocketError>;

    fn receive_typed<T:TryIntoFromBuffer>(&mut self, flags:OpFlag) -> Result<TypedMessage<T>, ReceiveTypedError<T>> {
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
    fn query(&mut self, message :RawMessage, flags:OpFlag) -> Result<RawMessage, SocketError> {
        self.send(message, flags.clone())?;
        self.receive(flags)
    }

    fn query_typed<T:TryIntoFromBuffer>(&mut self, message:TypedMessage<T>, flags:OpFlag) -> Result<TypedMessage<T>, QueryTypedError<T>> {
        match self.send_typed(message, flags.clone()) {
            Ok(_) => match self.receive_typed(flags) {
                Ok(msg) => Ok(msg),
                Err(err) => Err(QueryTypedError::Receive(err))
            },
            Err(err) => Err(QueryTypedError::Send(err))
        }
    }

    fn respond<T: Fn(RawMessage) -> RawMessage> (&mut self, flags:OpFlag, processor: T) -> Result<(), SocketError> {
        let query = self.receive(flags.clone())?;
        let query_metadata = query.metadata().clone();
        let response = processor(query).continue_exchange_metadata(query_metadata);
        self.send(response, flags)?;
        Ok(())
    }

    fn respond_typed<T:TryIntoFromBuffer, Q: Fn(TypedMessage<T>) -> TypedMessage<T>>(&mut self, flags:OpFlag, processor: Q) -> Result<(), QueryTypedError<T>> {
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
use std::convert::{From};
use std::io;
use core::socket::{SocketError, ConnectorError};

#[derive(Debug)]
pub enum State {
    Empty,
    Remainder,
    Stream(SocketError)
}

impl From<io::Error> for SocketError {
    fn from(error: io::Error) -> Self {
        println!("{}: {:?}", error, error.kind());
        match error.kind() {
            io::ErrorKind::AddrInUse => SocketError::TransportMethodAlreadyInUse,
            io::ErrorKind::AddrNotAvailable => SocketError::TransportTargetUnreachable,
            io::ErrorKind::AlreadyExists => SocketError::TransportMethodAlreadyInUse,
            io::ErrorKind::BrokenPipe => SocketError::Disconnected,
            io::ErrorKind::ConnectionAborted => SocketError::Disconnected,
            io::ErrorKind::ConnectionRefused => SocketError::ConnectionRefused,
            io::ErrorKind::Interrupted => SocketError::Disconnected,
            io::ErrorKind::InvalidData => SocketError::InternalError,
            io::ErrorKind::InvalidInput => SocketError::InternalError,
            io::ErrorKind::NotConnected => SocketError::Disconnected,
            io::ErrorKind::TimedOut => SocketError::Timeout,
            io::ErrorKind::Other => SocketError::InternalError,
            _ => SocketError::InternalError,
        }
    }
}

impl From<io::Error> for ConnectorError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::AddrInUse => ConnectorError::AlreadyInUse,
            io::ErrorKind::AddrNotAvailable => ConnectorError::CouldNotConnect,
            io::ErrorKind::AlreadyExists => ConnectorError::AlreadyConnected,
            io::ErrorKind::BrokenPipe => ConnectorError::InternalError,
            io::ErrorKind::ConnectionAborted => ConnectorError::InternalError,
            io::ErrorKind::ConnectionRefused => ConnectorError::InternalError,
            io::ErrorKind::Interrupted => ConnectorError::InternalError,
            io::ErrorKind::InvalidData => ConnectorError::InternalError,
            io::ErrorKind::InvalidInput => ConnectorError::InternalError,
            io::ErrorKind::NotConnected => ConnectorError::CouldNotConnect,
            io::ErrorKind::TimedOut => ConnectorError::InternalError,
            io::ErrorKind::Other => ConnectorError::InternalError,
            _ => ConnectorError::InternalError,
        }
    }
}

impl From<io::Error> for State {
    fn from(error: io::Error) -> Self {
        State::Stream(SocketError::from(error))
    }
}

impl From<SocketError> for State {
    fn from(error: SocketError) -> Self {
        State::Stream(error)
    }
}
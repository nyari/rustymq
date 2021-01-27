use std::convert::{From};
use std::io;
use core::socket::{SocketInternalError};

#[derive(Debug)]
pub enum State {
    Empty,
    Remainder,
    Stream(SocketInternalError)
}

impl From<io::Error> for SocketInternalError {
    fn from(error: io::Error) -> Self {
        println!("{}: {:?}", error, error.kind());
        match error.kind() {
            io::ErrorKind::AddrInUse => SocketInternalError::TransportMethodAlreadyInUse,
            io::ErrorKind::AddrNotAvailable => SocketInternalError::TransportTargetUnreachable,
            io::ErrorKind::AlreadyExists => SocketInternalError::TransportMethodAlreadyInUse,
            io::ErrorKind::BrokenPipe => SocketInternalError::Disconnected,
            io::ErrorKind::ConnectionAborted => SocketInternalError::Disconnected,
            io::ErrorKind::ConnectionRefused => SocketInternalError::ConnectionRefused,
            io::ErrorKind::Interrupted => SocketInternalError::Disconnected,
            io::ErrorKind::InvalidData => SocketInternalError::UnknownInternalError,
            io::ErrorKind::InvalidInput => SocketInternalError::UnknownInternalError,
            io::ErrorKind::NotConnected => SocketInternalError::Disconnected,
            io::ErrorKind::TimedOut => SocketInternalError::Timeout,
            io::ErrorKind::Other => SocketInternalError::UnknownInternalError,
            _ => SocketInternalError::UnknownInternalError,
        }
    }
}

impl From<io::Error> for State {
    fn from(error: io::Error) -> Self {
        State::Stream(SocketInternalError::from(error))
    }
}

impl From<SocketInternalError> for State {
    fn from(error: SocketInternalError) -> Self {
        State::Stream(error)
    }
}
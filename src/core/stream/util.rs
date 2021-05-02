use core::socket::SocketInternalError;
use core::stream::tracker::TrackingError;
use std::convert::From;
use std::io;

#[derive(Debug)]
pub enum State {
    Empty,
    Remainder,
    Stream(SocketInternalError),
}

impl From<io::Error> for SocketInternalError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::AddrInUse => SocketInternalError::TransportMethodAlreadyInUse,
            io::ErrorKind::AddrNotAvailable => SocketInternalError::TransportTargetUnreachable,
            io::ErrorKind::AlreadyExists => SocketInternalError::TransportMethodAlreadyInUse,
            io::ErrorKind::BrokenPipe => SocketInternalError::Disconnected,
            io::ErrorKind::ConnectionAborted => SocketInternalError::Disconnected,
            io::ErrorKind::ConnectionRefused => SocketInternalError::ConnectionRefused,
            io::ErrorKind::ConnectionReset => SocketInternalError::Disconnected,
            io::ErrorKind::Interrupted => SocketInternalError::Disconnected,
            io::ErrorKind::NotConnected => SocketInternalError::Disconnected,
            io::ErrorKind::TimedOut => SocketInternalError::Timeout,
            _ => SocketInternalError::UnknownInternalError(format!(
                "Stream error that cannot be handled: {:?}",
                error.kind()
            )),
        }
    }
}

impl From<TrackingError> for State {
    fn from(error: TrackingError) -> Self {
        match error {
            TrackingError::ReceiptError => {
                State::Stream(SocketInternalError::TransportIntegrityError)
            }
            TrackingError::IncompatibleHeaderVersion => {
                State::Stream(SocketInternalError::IncompatiblePeer)
            }
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

use crate::base::queue::MessageQueueError;
use crate::base::socket::{OpFlag, SocketError};
use crate::internals::queue::ReceiptState;

use std::string::String;
/// # [`Socket`] errors
/// Contains the possible errors for a socket that may need to be handled internally or by the user
#[derive(Debug, Clone)]
pub enum SocketInternalError {
    IncorrectStateError,
    UnrelatedPeer,
    UnrelatedConversation,
    UnrelatedConversationPart,
    QueueDepthReached,
    TransportIntegrityError,
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
    UnknownInternalError(String),
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

impl From<MessageQueueError> for SocketInternalError {
    fn from(input: MessageQueueError) -> SocketInternalError {
        match input {
            MessageQueueError::ReceiversAllDropped | MessageQueueError::SendersAllDropped => {
                SocketInternalError::Disconnected
            }
            MessageQueueError::QueueFull => SocketInternalError::QueueDepthReached,
            _ => SocketInternalError::UnknownInternalError(format!(
                "Could not convert to SocketInternalError: {:?}",
                input
            )),
        }
    }
}

impl From<ReceiptState> for SocketInternalError {
    fn from(input: ReceiptState) -> SocketInternalError {
        match input {
            ReceiptState::Dropped => SocketInternalError::Disconnected,
            _ => SocketInternalError::UnknownInternalError(format!(
                "Could not convert to SocketInternalError: {:?}",
                input
            )),
        }
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
            SocketInternalError::TransportIntegrityError => SocketError::TransportIntegrityError,
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

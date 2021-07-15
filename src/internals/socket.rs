use crate::base::queue::MessageQueueError;
use crate::base::socket::{OpFlag, SocketError};
use crate::internals::queue::ReceiptState;

use std::string::String;
/// # [`crate::base::Socket`] errors
/// Contains the possible errors for a socket that may need to be handled internally or by the user
#[derive(Debug, Clone)]
pub enum SocketInternalError {
    SocketIncorrectStateError,
    SocketUnrelatedPeer,
    SocketUnrelatedConversation,
    SocketUnrelatedConversationPart,
    SocketConversationIdentifierMissing,
    TransportQueueDepthReached,
    TransportIntegrityFatalError,
    SocketUnknownPeer,
    TransportMethodAlreadyInUse,
    TransportMethodTargetUnreachable,
    Disconnected,
    ConnectionRefused,
    Timeout,
    IncompatiblePeer,
    TransportHandshakeFailed,
    UnsupportedOpFlag(OpFlag),
    TransportMissingDNSDomainName,
    TransportMethodNotSupported,
    AlreadyConnected,
    NotSupportedOperation,
    TransoportCouldNotConnect,

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
            MessageQueueError::QueueFull => SocketInternalError::TransportQueueDepthReached,
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
            SocketInternalError::SocketIncorrectStateError => {
                SocketError::SocketIncorrectStateError
            }
            SocketInternalError::SocketUnrelatedPeer => SocketError::SocketUnrelatedPeer,
            SocketInternalError::SocketUnrelatedConversation => {
                SocketError::SocketUnrelatedConversation
            }
            SocketInternalError::SocketUnrelatedConversationPart => {
                SocketError::SocketUnrelatedConversationPart
            }
            SocketInternalError::SocketConversationIdentifierMissing => {
                SocketError::SocketConversationIdentifierMissing
            }
            SocketInternalError::TransportQueueDepthReached => {
                SocketError::TransportQueueDepthReached
            }
            SocketInternalError::TransportIntegrityFatalError => {
                SocketError::TransportIntegrityFatalError
            }
            SocketInternalError::SocketUnknownPeer => SocketError::SocketUnknownPeer,
            SocketInternalError::TransportMethodAlreadyInUse => {
                SocketError::TransportMethodAlreadyInUse
            }
            SocketInternalError::TransportMethodTargetUnreachable => {
                SocketError::TransportMethodTargetUnreachable
            }
            SocketInternalError::Disconnected => SocketError::Disconnected,
            SocketInternalError::ConnectionRefused => SocketError::ConnectionRefused,
            SocketInternalError::Timeout => SocketError::Timeout,
            SocketInternalError::IncompatiblePeer => SocketError::IncompatiblePeer,
            SocketInternalError::TransportHandshakeFailed => SocketError::TransportHandshakeFailed,
            SocketInternalError::UnsupportedOpFlag(op_flag) => {
                SocketError::UnsupportedOpFlag(op_flag)
            }
            SocketInternalError::TransportMissingDNSDomainName => {
                SocketError::TransportMissingDNSDomainName
            }
            SocketInternalError::TransportMethodNotSupported => {
                SocketError::TransportMethodNotSupported
            }
            SocketInternalError::AlreadyConnected => SocketError::AlreadyConnected,
            SocketInternalError::NotSupportedOperation => SocketError::NotSupportedOperation,
            SocketInternalError::TransoportCouldNotConnect => {
                SocketError::TransoportCouldNotConnect
            }
            err => panic!(
                "SocketInternalError occured that cannot be converted to SocketError: {:?}",
                err
            ),
        }
    }
}

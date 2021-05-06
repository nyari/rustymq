use libc;

use crate::base::message::{RawMessage};
use crate::internals::serializer::{BareFlatSerializer, BareFlatDeserializer, Serializable, Serializer, Deserializer};
use crate::internals::serializer;

use std::{path::Path};

#[derive(Debug, Clone)]
pub enum SystemVMessageError {
    MissingPath,
    ExclusiveCreationImpossible,
    PermissionError,
    SymbolicLinkEncountered,
    PathnameTooLong,
    PathIncorrect,
    DidNotCreate,
    OutOfMemory,
    SystemMessageQueueMaximumReached,
    UnkownError,
}

impl SystemVMessageError {
    fn errno() -> libc::c_int {
        unsafe { *libc::__errno_location() }
    }

    pub fn from_libc_stat_errno() -> Self {
        match Self::errno() {
            libc::EACCES => SystemVMessageError::PermissionError,
            libc::ELOOP => SystemVMessageError::SymbolicLinkEncountered,
            libc::ENAMETOOLONG => SystemVMessageError::PathnameTooLong,
            libc::ENOENT | libc::ENOTDIR => SystemVMessageError::PathIncorrect,
            _ => SystemVMessageError::UnkownError
        }
    }

    pub fn from_libc_msgget_errno() -> Self {
        match Self::errno() {
            libc::EACCES => SystemVMessageError::PermissionError,
            libc::EEXIST => SystemVMessageError::ExclusiveCreationImpossible,
            libc::ENOENT => SystemVMessageError::DidNotCreate,
            libc::ENOMEM => SystemVMessageError::OutOfMemory,
            libc::ENOSPC => SystemVMessageError::SystemMessageQueueMaximumReached,
            _ => SystemVMessageError::UnkownError
        }
    }
}

struct RawPayload {
    message_type: libc::c_long,
    queue_id: libc::c_int,
    message: RawMessage
}

impl Serializable for RawPayload {
    /// Serialize `self` into `serializer`
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize_raw(&self.message_type);
        serializer.serialize_raw(&self.queue_id);
        serializer.serialize(&self.message);
    }

    /// Deserialize a value with the type of `Self` from a deserializer
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self {
            message_type: deserializer.deserialize()?,
            queue_id: deserializer.deserialize()?,
            message: deserializer.deserialize()?
        })
    }
}

struct SystemVMessageInterface {
    token: libc::key_t,
    queue_id: libc::c_int,
}

impl SystemVMessageInterface {
    pub fn new(path: &Path, proj_id: libc::c_int) -> Result<Self, SystemVMessageError> {
        unsafe {
            let token = libc::ftok(std::mem::transmute(path.to_str().ok_or(SystemVMessageError::MissingPath)?.as_ptr()), proj_id);
            if token != -1 {
                let queue_id = libc::msgget(token, libc::IPC_CREAT);
                if queue_id != -1 {
                    Ok(Self {
                        token: token,
                        queue_id: queue_id
                    })
                } else {
                    Err(SystemVMessageError::from_libc_msgget_errno())
                }
            } else {
                Err(SystemVMessageError::from_libc_stat_errno())
            }
        }
    }
}
//! # Data module
//! This module is used to define the datastructure between the client and server and implements serialization

use rustymq::core::message::SerializableMessagePayload;

/// For serialization in this example the internal serializer of RusyMQ is used. Although it is possible to use
/// any serializaton method or library.
use rustymq::core::serializer::{Serializable, Serializer, Deserializer, FlatSerializer, FlatDeserializer, Error};
use rustymq::base::message::{Buffer, BufferSlice};
use super::time::{Duration};
use std::time;

/// Create a Serializable trait locally is well because we cannot implement Serializable trait from external crate on Duration from external crate (E0117)
trait LocalSerializable : Sized {
    fn serialize<T:Serializer>(&self, serializer: &mut T);
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, Error>;
}

#[derive(Debug)]
/// # Operation Task
/// Is the communication structure used to comminucate an operation task to the server
pub enum OperationTask {
    /// Add two 32-bit integers
    Addition(i32, i32),
    /// Multiply two 32-bit integers
    Multiplication(i32, i32)
}

#[derive(Debug)]
/// # Operation result
/// Is the communication structure used to communicate the result of an [`OperationTask`] to the client
pub enum OperationResult {
    Success(i32),
    Error
}

/// Implement serialization for [`OperationTask`]
impl Serializable for OperationTask {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        match self {
            Self::Addition(v1, v2) => {
                serializer.serialize_raw(&0u8);
                serializer.serialize_raw(v1);
                serializer.serialize_raw(v2);
            },
            Self::Multiplication(v1, v2) => {
                serializer.serialize_raw(&1u8);
                serializer.serialize_raw(v1);
                serializer.serialize_raw(v2);

            }
        }
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
        match deserializer.deserialize_raw::<u8>()? {
            0u8 => {
                Ok(Self::Addition(deserializer.deserialize_raw::<i32>()?, deserializer.deserialize_raw::<i32>()?))
            },
            1u8 => {
                Ok(Self::Multiplication(deserializer.deserialize_raw::<i32>()?, deserializer.deserialize_raw::<i32>()?))
            },
            _ => Err(Error::DemarshallingFailed)
        }
    }
}

/// Implement serialization for [`OperationTask`]
impl Serializable for OperationResult {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        match self {
            OperationResult::Success(v1 ) => {
                serializer.serialize_raw(&0u8);
                serializer.serialize_raw(v1);
            },
            OperationResult::Error => {
                serializer.serialize_raw(&1u8);
            }
        }
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
        match deserializer.deserialize_raw::<u8>()? {
            0u8 => {
                Ok(Self::Success(deserializer.deserialize_raw::<i32>()?))
            },
            1u8 => {
                Ok(Self::Error)
            },
            _ => Err(Error::DemarshallingFailed)
        }
    }
}

#[derive(Debug)]
pub struct TimedOperation<T>(pub T, pub Duration) where T: Serializable;

impl<T> SerializableMessagePayload for TimedOperation<T>
    where T: Serializable {
    type SerializationError = ();
    type DeserializationError = Error;

    fn serialize(self) -> Result<Buffer, Self::SerializationError> {
        let mut serializer = FlatSerializer::new();
        serializer.serialize(&self.0);
        serializer.serialize::<u64>(&self.1.as_secs());
        serializer.serialize::<u32>(&self.1.subsec_nanos());
        Ok(serializer.finalize())
    }
    fn deserialize<'a>(buffer: BufferSlice<'a>) -> Result<Self, Self::DeserializationError> {
        let mut deserializer = FlatDeserializer::new(buffer)?;
        let operation: T = deserializer.deserialize()?;
        let dur_secs: u64 = deserializer.deserialize()?;
        let dur_nanos: u32 = deserializer.deserialize()?;
        Ok(Self(operation, Duration::new(time::Duration::new(dur_secs, dur_nanos))))
    }
}
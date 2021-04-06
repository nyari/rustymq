//! # Data module
//! This module is used to define the datastructure between the client and server and implements serialization

use std::convert::TryFrom;
use std::time::{Duration};

/// For serialization in this example the internal serializer of RusyMQ is used. Although it is possible to use
/// any serializaton method or library.
use rustymq::core::serializer::{Serializable, Serializer, Deserializer, FlatSerializer, FlatDeserializer, Error, Buffer};

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

/// Implement serializable for Duration
impl LocalSerializable for Duration {
    /// Serialize `self` into `serializer`
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize_raw(&self.as_secs());
        serializer.serialize_raw(&self.subsec_nanos());
    }
    /// Deserialize a value with the type of `Self` from a deserializer
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, Error> {
        Ok(Self::new(deserializer.deserialize_raw()?, deserializer.deserialize_raw()?))
    }
}

impl<T, U> LocalSerializable for (T, U) 
    where T: Serializable,
          U: Serializable {

}

/// Implement the TryFrom<Buffer> for OperationTask to be able to use it in [`TypedMessage`]s. 
impl TryFrom<Buffer> for OperationTask {
    type Error = Error;
    fn try_from(value: Buffer) -> Result<Self, Self::Error> {
        let mut deserializer = FlatDeserializer::new(value.as_slice())?;
        deserializer.deserialize::<Self>()
    }
}

/// Implement the TryFrom<Buffer> for OperationTask to be able to use it in [`TypedMessage`]s. 
impl TryFrom<OperationTask> for Buffer {
    type Error = Error;
    fn try_from(value: OperationTask) -> Result<Self, Self::Error> {
        let mut serializer = FlatSerializer::new();
        serializer.serialize_pass(value);
        Ok(serializer.finalize())
    }
}

/// Implement the TryFrom<Buffer> for OperationResult to be able to use it in [`TypedMessage`]s. 
impl TryFrom<Buffer> for OperationResult {
    type Error = Error;
    fn try_from(value: Buffer) -> Result<Self, Error> {
        let mut deserializer = FlatDeserializer::new(value.as_slice())?;
        deserializer.deserialize::<Self>()
    }
}

/// Implement the TryFrom<Buffer> for OperationResult to be able to use it in [`TypedMessage`]s. 
impl TryFrom<OperationResult> for Buffer {
    type Error = Error;
    fn try_from(value: OperationResult) -> Result<Self, Error> {
        let mut serializer = FlatSerializer::new();
        serializer.serialize_pass(value);
        Ok(serializer.finalize())
    }
}
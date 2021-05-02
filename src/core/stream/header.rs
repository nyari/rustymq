use core::message::{RawMessage};
use core::serializer::{Serializable, Serializer, Deserializer};
use core::serializer;
const STREAM_HEADER_VERSION:u8 = 0u8;

pub enum HeaderOperation {
    Payload(RawMessage),
    Heartbeat,
    Receipt,
}

impl Serializable for HeaderOperation {
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        match self {
            HeaderOperation::Payload(message) => {
                serializer.serialize(&0u8);
                serializer.serialize(message);
            },
            HeaderOperation::Heartbeat => {
                serializer.serialize(&1u8);
            },
            HeaderOperation::Receipt => {
                serializer.serialize(&2u8);
            }
        }
    }
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        match deserializer.deserialize::<u8>()? {
            0u8 => Ok(HeaderOperation::Payload(deserializer.deserialize::<RawMessage>()?)),
            1u8 => Ok(HeaderOperation::Heartbeat),
            2u8 => Ok(HeaderOperation::Receipt),
            _ => Err(serializer::Error::DemarshallingFailed)
        }
    }
}

pub struct Header {
    version: u8,
    sequence: u32
}

impl Header {
    pub fn new(sequence: u32) -> Self {
        Self {
            version: STREAM_HEADER_VERSION,
            sequence: sequence
        }
    }

    pub fn compatible(&self) -> bool {
        self.version == STREAM_HEADER_VERSION
    }

    pub fn sequence(&self) -> u32 {
        self.sequence
    }
}

impl Serializable for Header {
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.version);
        serializer.serialize(&self.sequence);
    }

    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self{
            version: deserializer.deserialize()?,
            sequence: deserializer.deserialize()?,
        })
    }
}

pub struct HeadedMessage {
    header: Header,
    operation: HeaderOperation
}

impl HeadedMessage {
    pub fn new(header: Header, operation: HeaderOperation) -> Self {
        Self {
            header: header,
            operation: operation,
        }
    }

    pub fn into_parts(self) -> (Header, HeaderOperation) {
        (self.header, self.operation)
    }
}

impl Serializable for HeadedMessage {
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.header);
        serializer.serialize(&self.operation);
    }

    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self{
            header: deserializer.deserialize()?,
            operation: deserializer.deserialize()?,
        })
    }
}
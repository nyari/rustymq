use core::message::{Buffer, BufferSlice, SerializableMessagePayload};
use core::serializer;
use core::serializer::{Deserializer, FlatDeserializer, FlatSerializer, Serializer};

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct TestingStruct {
    pub a: u64,
    pub b: u64,
}

impl SerializableMessagePayload for TestingStruct {
    type SerializationError = ();
    type DeserializationError = serializer::Error;

    fn serialize(self) -> Result<Buffer, Self::SerializationError> {
        let mut serializer = FlatSerializer::new();
        serializer.serialize_raw(&self.a);
        serializer.serialize_raw(&self.b);
        Ok(serializer.finalize())
    }

    fn deserialize<'a>(buffer: BufferSlice<'a>) -> Result<Self, Self::DeserializationError> {
        let mut deserializer = FlatDeserializer::new(buffer)?;

        Ok(Self {
            a: deserializer.deserialize_raw::<u64>()?,
            b: deserializer.deserialize_raw::<u64>()?,
        })
    }
}

use super::State;
use core::message::Buffer;
use core::serializer;
use core::serializer::{FlatDeserializer, Serializable};
use core::socket::SocketInternalError;

use std::io::Read;

pub struct StreamSerializableReader {
    buffer: Buffer,
    batch_size: usize,
    len: usize,
    capacity: usize,
}

impl StreamSerializableReader {
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: Buffer::with_capacity(batch_size),
            batch_size: batch_size,
            capacity: batch_size,
            len: 0,
        }
    }

    fn try_parse_raw_message<T: Serializable>(&mut self) -> Result<T, SocketInternalError> {
        let (mut deserializer, actual_bytes) = match FlatDeserializer::new(&self.buffer[..self.len])
        {
            Ok(result) => Ok((result, self.len)),
            Err(serializer::Error::IncorrectBufferSize(actual_size)) => {
                if self.len < actual_size as usize {
                    Err(SocketInternalError::IncompleteData)
                } else if self.len > actual_size as usize {
                    match FlatDeserializer::new(&self.buffer[..actual_size as usize]) {
                        Ok(result) => Ok((result, actual_size as usize)),
                        Err(_) => Err(SocketInternalError::UnknownDataFormatReceived),
                    }
                } else {
                    Err(SocketInternalError::UnknownDataFormatReceived)
                }
            }
            Err(serializer::Error::EndOfBuffer) => Err(SocketInternalError::IncompleteData),
            _ => Err(SocketInternalError::UnknownDataFormatReceived),
        }?;

        match T::deserialize(&mut deserializer) {
            Ok(message) => {
                self.buffer.copy_within(actual_bytes.., 0usize);
                self.len = self.len - actual_bytes;
                self.capacity = self.len;
                self.buffer.truncate(self.len);
                Ok(message)
            }
            Err(serializer::Error::DemarshallingFailed)
            | Err(serializer::Error::ByteOrderMarkError) => {
                Err(SocketInternalError::UnknownDataFormatReceived)
            }
            Err(serializer::Error::EndOfBuffer) => Err(SocketInternalError::UnknownInternalError(
                "Could not parse incoming message from stream".to_string(),
            )),
            _ => panic!("Any other case should already have been handled"),
        }
    }

    fn try_parse_buffer<T: Serializable>(&mut self) -> Result<Vec<T>, State> {
        if self.len > 0 {
            let mut outputs = Vec::new();
            let last_error = loop {
                match self.try_parse_raw_message() {
                    Ok(message) => outputs.push(message),
                    Err(other) => break other,
                }
            };

            match last_error {
                SocketInternalError::IncompleteData => {
                    if outputs.is_empty() {
                        Err(State::Remainder)
                    } else {
                        Ok(outputs)
                    }
                }
                SocketInternalError::UnknownInternalError(message) => panic!("{}", message),
                other => Err(State::from(other)),
            }
        } else {
            Err(State::Empty)
        }
    }

    pub fn read_into<T: Serializable, F: Read>(&mut self, reader: &mut F) -> Result<Vec<T>, State> {
        self.ensure_batch_size_capacity_at_end_of_buffer();
        match self.read_into_buffer(reader) {
            Ok(()) | Err(State::Empty) => self.try_parse_buffer(),
            Err(other) => Err(other),
        }
    }

    fn ensure_batch_size_capacity_at_end_of_buffer<'a>(&'a mut self) {
        self.capacity = std::cmp::max(self.capacity, self.len + self.batch_size);
        if self.capacity > self.buffer.len() {
            self.buffer.resize(self.capacity, 0u8);
        }
    }

    fn read_into_buffer<'a, F: Read>(&mut self, reader: &mut F) -> Result<(), State> {
        match reader.read(&mut self.buffer[self.len..self.len + self.batch_size]) {
            Ok(amount) => {
                if amount != 0 {
                    self.len += amount;
                    Ok(())
                } else {
                    Err(State::Empty)
                }
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::WouldBlock => Ok(()),
                std::io::ErrorKind::Interrupted => Ok(()),
                _ => Err(State::from(err)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::message::{Message, RawMessage};
    use core::serializer::{FlatSerializer, Serializer};
    use std::io::Cursor;

    impl PartialEq for RawMessage {
        fn eq(&self, other: &Self) -> bool {
            self.metadata() == other.metadata() && self.payload() == other.payload()
        }
    }

    impl From<RawMessage> for Buffer {
        fn from(message: RawMessage) -> Self {
            let mut ser = FlatSerializer::new();
            ser.serialize_pass(message);
            ser.finalize()
        }
    }

    fn read_all_messages(
        mut cursor: Cursor<Buffer>,
        mut reader: StreamSerializableReader,
    ) -> Vec<RawMessage> {
        let mut results = Vec::new();
        loop {
            match reader.read_into(&mut cursor) {
                Ok(message) => results.extend(message.into_iter()),
                Err(State::Empty) => break results,
                Err(State::Remainder) => (),
                _ => panic!("Internal error"),
            }
        }
    }

    #[test]
    fn test_single_message_read_2048_batch_size() {
        let original_message = RawMessage::new(vec![0xAA, 0x01, 0x02, 0x03, 0x34]);
        let mut cursor: Cursor<Buffer> = Cursor::new(original_message.clone().into());
        let mut reader = StreamSerializableReader::new(2048);
        let result: Vec<RawMessage> = reader.read_into(&mut cursor).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], original_message);
    }

    #[test]
    fn test_single_message_read_8_batch_size() {
        let original_message = RawMessage::new(vec![0xAA, 0x01, 0x02, 0x03, 0x34]);
        let cursor: Cursor<Buffer> = Cursor::new(original_message.clone().into());
        let reader = StreamSerializableReader::new(8);
        let result = read_all_messages(cursor, reader);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], original_message);
    }

    #[test]
    fn test_two_message_read_2048_batch_size() {
        let original_message = RawMessage::new(vec![0xAA, 0x01, 0x02, 0x03, 0x34]);
        let mut buffer: Buffer = original_message.clone().into();
        buffer.append(&mut original_message.clone().into());

        let mut cursor: Cursor<Buffer> = Cursor::new(buffer);
        let mut reader = StreamSerializableReader::new(2048);
        let result: Vec<RawMessage> = reader.read_into(&mut cursor).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], original_message);
        assert_eq!(result[1], original_message);
    }

    #[test]
    fn test_two_message_read_8_batch_size() {
        let original_message = RawMessage::new(vec![0xAA, 0x01, 0x02, 0x03, 0x34]);
        let mut buffer: Buffer = original_message.clone().into();
        buffer.append(&mut original_message.clone().into());

        let cursor: Cursor<Buffer> = Cursor::new(buffer);
        let reader = StreamSerializableReader::new(8);
        let result = read_all_messages(cursor, reader);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], original_message);
        assert_eq!(result[1], original_message);
    }
}

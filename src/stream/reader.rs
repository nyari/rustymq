use core::socket::{SocketError};
use core::message::{RawMessage, Buffer};
use core::serializer::{Serializable, FlatDeserializer};
use core::serializer;
use super::{State};

use std::io::{Read};

pub struct RawMessageReader {
    buffer: Buffer,
    batch_size: usize,
    len: usize,
    capacity: usize
}

impl RawMessageReader {
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: Buffer::with_capacity(batch_size),
            batch_size: batch_size,
            capacity: batch_size,
            len: 0
        }
    }

    pub fn try_parse_raw_message(&mut self) -> Result<RawMessage, SocketError> {
        let (mut deserializer, actual_bytes) = match FlatDeserializer::new(&self.buffer[..self.len]) {
            Ok(result) => Ok((result, self.len)),
            Err(serializer::Error::IncorrectBufferSize(actual_size)) => {
                if self.len < actual_size as usize {
                    Err(SocketError::IncompleteData)
                } else if self.len > actual_size as usize {
                    match FlatDeserializer::new(&self.buffer[..actual_size as usize]) {
                        Ok(result) => Ok((result, actual_size as usize)),
                        Err(_) => Err(SocketError::UnknownDataFormatReceived)
                    }
                } else {
                    Err(SocketError::UnknownDataFormatReceived)
                }
            }
            Err(serializer::Error::EndOfBuffer) => Err(SocketError::IncompleteData),
            _ => Err(SocketError::UnknownDataFormatReceived),
        }?;

        match RawMessage::deserialize(&mut deserializer) {
            Ok(message) => {
                self.buffer.copy_within(actual_bytes.., 0usize);
                self.len = self.len - actual_bytes;
                self.capacity = self.len;
                self.buffer.truncate(self.len);
                Ok(message)
            }
            Err(serializer::Error::DemarshallingFailed) | Err(serializer::Error::ByteOrderMarkError) => Err(SocketError::UnknownDataFormatReceived),
            Err(serializer::Error::EndOfBuffer) => Err(SocketError::InternalError),
            _ => panic!("Any other case should already have been handled")
        }
    }

    pub fn read_into<F: Read>(&mut self, reader:&mut F) -> Result<(), State>{
        self.ensure_batch_size_capacity_at_end_of_buffer();
        self.read_into_buffer(reader)
    }

    fn ensure_batch_size_capacity_at_end_of_buffer<'a>(&'a mut self) {
        self.capacity = std::cmp::max(self.capacity, self.len + self.batch_size);
        if self.capacity > self.buffer.len() {
            self.buffer.resize(self.capacity, 0u8);
        }
    }

    fn read_into_buffer<'a, F: Read>(&mut self, reader:&mut F) -> Result<(), State> {
        match reader.read(&mut self.buffer[self.len..self.len+self.batch_size]) {
            Ok(amount) => {
                if amount != 0 {
                    self.len += amount;
                    Ok(())
                } else {
                    Err(State::Stream(SocketError::Timeout))
                }
            },
            Err(err) => match err.kind() {
                std::io::ErrorKind::TimedOut => Err(State::Stream(SocketError::Timeout)),
                std::io::ErrorKind::WouldBlock => Err(State::Stream(SocketError::Timeout)),
                _ => Err(State::from(err))
            }
        }
    }
}

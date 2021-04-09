use core::message::{RawMessage, Buffer};
use core::serializer::{Serializer, FlatSerializer, BufferSlice};
use core::util::thread::{ChgNtfMutex};
use std::sync::{Arc};

use super::{State};

use std::io::{Write};

pub struct RawMessageWriter {
    buffer: Buffer,
    semaphore: Option<Arc<ChgNtfMutex<bool>>>,
    batch_size: usize,
    offset: usize
}

impl RawMessageWriter {
    pub fn new(message: RawMessage, batch_size: usize) -> Self {
        Self {
            buffer: {let mut serializer = FlatSerializer::new();
                     serializer.serialize(&message);
                     serializer.finalize()},
            semaphore: None,
            batch_size: batch_size,
            offset: 0
        }
    }

    pub fn new_with_semaphore(message: RawMessage, batch_size: usize, semaphore: Arc<ChgNtfMutex<bool>>) -> Self {
        Self {
            buffer: {let mut serializer = FlatSerializer::new();
                     serializer.serialize(&message);
                     serializer.finalize()},
            semaphore: Some(semaphore),
            batch_size: batch_size,
            offset: 0
        }
    }

    pub fn new_empty() -> Self {
        Self {
            buffer: Buffer::new(),
            semaphore: None,
            batch_size: 0,
            offset: 0
        }
    }

    fn get_batch<'a>(&'a self) -> Option<BufferSlice<'a>> {
        if self.offset + self.batch_size < self.buffer.len() {
            Some(&self.buffer[self.offset..self.offset+self.batch_size])
        } else if self.offset < self.buffer.len() {
            Some(&self.buffer[self.offset..])
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.offset >= self.buffer.len()
    }

    fn progress_amount(&mut self, amount: usize) {
        if amount <= self.batch_size {
            self.offset += amount;
            if self.is_empty() {
                if let Some(finished_semaphore) = self.semaphore.take() {
                    *finished_semaphore.lock_notify().unwrap() = true;
                }
            }
        } else {
            panic!("Cannot push back more than batch size")
        }
    }

    pub fn write_into<F: Write>(&mut self, writer:&mut F)-> Result<(), State> {
        let result = match self.get_batch() {
            Some(buffer_slice) => Ok(writer.write(buffer_slice)?),
            None => Err(State::Empty)
        };

        match result {
            Ok(processed_amount) => {
                self.progress_amount(processed_amount);
                if self.is_empty() {
                    Err(State::Empty)
                } else {
                    Ok(())
                }
            },
            Err(err) => {
                Err(err)
            }
        }
    }
}
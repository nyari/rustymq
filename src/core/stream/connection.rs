use core::message::{Message, PeerId, RawMessage};
use core::stream::header::{HeadedMessage};
use core::stream::tracker::{Tracker};
use core::queue::{MessageQueueError, MessageQueueReceiver, MessageQueueSender, SenderReceipt};
use core::socket::SocketInternalError;
use core::stream;
use core::util::thread::{Semaphore, Sleeper};
use core::util::time::{DurationBackoffWithDebounce, LinearDurationBackoff};

use std::collections::VecDeque;
use std::io;
use std::thread;
use std::time::Duration;

const BUFFER_BATCH_SIZE: usize = 2048;

fn query_thread_default_duration_backoff() -> DurationBackoffWithDebounce<LinearDurationBackoff> {
    DurationBackoffWithDebounce::new(
        LinearDurationBackoff::new(Duration::from_millis(0), Duration::from_millis(500), 20),
        500000,
    )
}

#[derive(Debug)]
pub enum ReadWriteStremConnectionState {
    Busy,
    Free,
}

impl ReadWriteStremConnectionState {
    pub fn is_free(&self) -> bool {
        matches!(self, ReadWriteStremConnectionState::Free)
    }
}

/// Handling queueing of messages to be send and received through [`io::Read`] and [`io::Write`] streams
/// # Tasks
/// * It constructs the outward message from which copies can be requestsed
/// * Inward queue is required to queue incoming messages to
/// * Using [`stream::RawMessageReader`] for reading messages from input stream
/// * Using [`stream::RawMessageWriter`] for writing messages to output stream
pub struct ReadWriteStreamConnection<S: io::Read + io::Write + Send> {
    stream: S,
    reader: stream::RawMessageReader,
    writer: stream::RawMessageWriter,
    outward_queue: MessageQueueReceiver<RawMessage>,
    inward_queue: MessageQueueSender<RawMessage>,
    peer_id: PeerId,
    last_received: VecDeque<RawMessage>,
    receipt_queue: VecDeque<HeadedMessage>,
    tracker: Tracker,
}

impl<S: io::Read + io::Write + Send> ReadWriteStreamConnection<S> {
    /// Creates an instance for a stream object and an inward queue
    pub fn new(
        stream: S,
        outward_queue: MessageQueueReceiver<RawMessage>,
        inward_queue: MessageQueueSender<RawMessage>,
        peer_id: PeerId,
    ) -> Self {
        Self {
            stream: stream,
            reader: stream::RawMessageReader::new(BUFFER_BATCH_SIZE),
            writer: stream::RawMessageWriter::new_empty(),
            outward_queue: outward_queue,
            inward_queue: inward_queue,
            peer_id: peer_id,
            last_received: VecDeque::new(),
            receipt_queue: VecDeque::new(),
            tracker: Tracker::new()
        }
    }

    /// Get a copy if the outward queue to post messages to
    pub fn get_outward_queue(&self) -> MessageQueueSender<RawMessage> {
        self.outward_queue.sender_pair()
    }

    /// Get a copy if the outward queue to post messages to
    pub fn get_inward_queue(&self) -> MessageQueueReceiver<RawMessage> {
        self.inward_queue.receiver_pair()
    }

    /// Read next chunk from the input stream into [`stream::RawMessageReader`]
    pub fn process_receiving(
        &mut self,
    ) -> Result<ReadWriteStremConnectionState, SocketInternalError> {
        match self.proceed_receiving() {
            Ok(()) => Ok(ReadWriteStremConnectionState::Busy),
            Err(stream::State::Remainder) => Ok(ReadWriteStremConnectionState::Busy),
            Err(stream::State::Empty) => Ok(ReadWriteStremConnectionState::Free),
            Err(stream::State::Stream(err)) => Err(err),
        }
    }

    /// Write next chunk of output into [`stream::RawMessageWriter`]
    pub fn process_sending(
        &mut self,
    ) -> Result<ReadWriteStremConnectionState, SocketInternalError> {
        match self.proceed_sending() {
            Ok(()) => Ok(ReadWriteStremConnectionState::Busy),
            Err(stream::State::Empty) => {
                if let Err(stream::State::Empty) = self.dequeue_next_outgoing_message_to_writer() {
                    Ok(ReadWriteStremConnectionState::Free)
                } else {
                    Ok(ReadWriteStremConnectionState::Busy)
                }
            }
            Err(stream::State::Stream(err)) => Err(err),
            Err(stream::State::Remainder) => Err(SocketInternalError::UnknownInternalError(
                "Unknown error while writing into Stream".to_string(),
            )),
        }
    }

    /// Take message from output queue and add it to [`stream::RawMessageWriter`]
    fn dequeue_next_outgoing_message_to_writer(&mut self) -> Result<(), stream::State> {
        match self.receipt_queue.pop_front() {
            Some(transport_receipt) => {
                match self.outward_queue.receive_async_with_receipt() {
                    Ok(Some((Some(receipt), message))) => Ok(self.writer =
                        stream::RawMessageWriter::new_with_receipt(self.tracker.head_message_and_receipt(message, transport_receipt.into_parts().0), BUFFER_BATCH_SIZE, receipt)),
                    Ok(Some((None, message))) => {
                        Ok(self.writer = stream::RawMessageWriter::new(self.tracker.head_message_and_receipt(message, transport_receipt.into_parts().0), BUFFER_BATCH_SIZE))
                    }
                    Ok(None) => {
                        self.writer = stream::RawMessageWriter::new(transport_receipt, BUFFER_BATCH_SIZE);
                        Err(stream::State::Remainder)
                    },
                    Err(err) => Err(stream::State::Stream(err.into())),
                }
            },
            None => {
                match self.outward_queue.receive_async_with_receipt() {
                    Ok(Some((Some(receipt), message))) => Ok(self.writer =
                        stream::RawMessageWriter::new_with_receipt(self.tracker.head_message(message), BUFFER_BATCH_SIZE, receipt)),
                    Ok(Some((None, message))) => {
                        Ok(self.writer = stream::RawMessageWriter::new(self.tracker.head_message(message), BUFFER_BATCH_SIZE))
                    }
                    Ok(None) => Err(stream::State::Empty),
                    Err(err) => Err(stream::State::Stream(err.into())),
                }
            }
        }

    }

    /// Handle writing a chunk of data into [`io::Write`] with [`stream::RawMessageWriter`]
    fn proceed_sending(&mut self) -> Result<(), stream::State> {
        self.writer.write_into(&mut self.stream)
    }

    /// Handle reading a chunk of data from [`io::Read`] with [`stream::RawMessageReader`]
    fn proceed_receiving(&mut self) -> Result<(), stream::State> {
        if self.last_received.is_empty() {
            let messages = self.reader.read_into(&mut self.stream)?;
            let peer_id = self.peer_id.clone();
            for headed_message in messages.into_iter() {
                let (message_opt, response_opt) = self.tracker.unwrap_incoming_message(headed_message)?;
                response_opt.into_iter().for_each(|response| self.receipt_queue.push_back(response));
                message_opt.into_iter().for_each(|message| {self.last_received.push_back(message.apply_peer_id(peer_id))});
            };
        }

        while !self.last_received.is_empty() {
            match self
                .inward_queue
                .send_unless_full(self.last_received.pop_front().unwrap())
            {
                Ok(_) => (),
                Err((MessageQueueError::QueueFull, message)) => {
                    self.last_received.push_front(message);
                    return Ok(());
                }
                Err((err, message)) => {
                    self.last_received.push_front(message);
                    return Err(stream::State::Stream(err.into()));
                }
            }
        }
        Ok(())
    }
}

/// Provides a main loop to continouosly send and receive [`RawMessage`]s through [`ReadWriteStreamConnection`]
/// This is mainnly intended to be used in a separate thread
pub struct ReadWriteStreamConnectionWorker<S: io::Read + io::Write + Send> {
    stream: ReadWriteStreamConnection<S>,
}

impl<S: io::Read + io::Write + Send> ReadWriteStreamConnectionWorker<S> {
    pub fn construct_from_stream(
        stream: ReadWriteStreamConnection<S>,
    ) -> Result<Self, SocketInternalError> {
        Ok(Self { stream: stream })
    }

    pub fn main_loop(mut self, stop_semaphore: Semaphore) -> Result<(), SocketInternalError> {
        let mut sleeper = Sleeper::new(query_thread_default_duration_backoff());
        loop {
            loop {
                let receiving = self.stream.process_receiving()?;
                let sending = self.stream.process_sending()?;
                if receiving.is_free() && sending.is_free() {
                    break;
                } else {
                    sleeper.reset();
                }
            }
            if stop_semaphore.is_signaled() {
                return Ok(());
            }
            sleeper.sleep();
        }
    }
}

pub struct ReadWriteStreamConnectionThreadManager {
    outward_queue: MessageQueueSender<RawMessage>,
    inward_queue: MessageQueueReceiver<RawMessage>,
    worker_thread: Option<std::thread::JoinHandle<Result<(), SocketInternalError>>>,
    last_error: Option<Result<(), SocketInternalError>>,
    stop_semaphore: Semaphore,
}

impl ReadWriteStreamConnectionThreadManager {
    pub fn execute_thread_for<S: io::Read + io::Write + Send + 'static>(
        stream: ReadWriteStreamConnection<S>,
    ) -> Result<Self, SocketInternalError> {
        let outward_queue = stream.get_outward_queue();
        let inward_queue = stream.get_inward_queue();
        let worker = ReadWriteStreamConnectionWorker::construct_from_stream(stream)?;
        let stop_semaphore = Semaphore::new();
        let stop_semaphore_clone = stop_semaphore.clone();
        Ok(Self {
            outward_queue: outward_queue,
            inward_queue: inward_queue,
            worker_thread: Some(thread::spawn(move || {
                worker.main_loop(stop_semaphore.clone())
            })),
            last_error: None,
            stop_semaphore: stop_semaphore_clone,
        })
    }

    pub fn check_worker_state(&mut self) -> Result<(), SocketInternalError> {
        if let Some(last_error) = self.last_error.as_ref() {
            last_error.clone()
        } else {
            if self.stop_semaphore.is_signaled() {
                let result = match self.worker_thread.take().unwrap().join() {
                    Ok(Ok(())) => Err(SocketInternalError::UnknownInternalError("A worker should not exit without an error condition except when it is explicitly stopped by the semaphore".to_string())),
                    Ok(error) => error,
                    Err(_) => Err(SocketInternalError::UnknownInternalError("Worker thread of socket has panicked or stopped unexpectedly".to_string()))
                };
                self.last_error = Some(result.clone());
                result
            } else {
                Ok(())
            }
        }
    }

    pub fn send_async(&mut self, message: RawMessage) -> Result<(), SocketInternalError> {
        self.check_worker_state()?;
        self.outward_queue.send(message)?;
        Ok(())
    }

    pub fn send(&mut self, message: RawMessage) -> Result<SenderReceipt, SocketInternalError> {
        self.check_worker_state()?;
        Ok(self.outward_queue.send_with_receipt(message)?)
    }

    pub fn receive_async(&mut self) -> Option<RawMessage> {
        self.inward_queue.receive_async().unwrap()
    }

    pub fn receive_async_all(&mut self) -> Vec<RawMessage> {
        self.inward_queue.receive_all().unwrap()
    }

    pub fn receive(&mut self) -> RawMessage {
        self.inward_queue.receive().unwrap()
    }
}

impl Drop for ReadWriteStreamConnectionThreadManager {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.last_error.is_none() {
            self.stop_semaphore.signal();
            match self.worker_thread.take() {
                Some(join_handle) => {
                    join_handle.join();
                }
                None => (),
            }
        }
    }
}

use core::message::RawMessage;
use core::queue::{InwardMessageQueue, OutwardMessageQueue};
use core::socket::SocketInternalError;
use core::stream;
use core::util::thread::{Semaphore, Sleeper};
use core::util::time::{DurationBackoffWithDebounce, LinearDurationBackoff};

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
    outward_queue: OutwardMessageQueue,
    inward_queue: InwardMessageQueue,
}

impl<S: io::Read + io::Write + Send> ReadWriteStreamConnection<S> {
    /// Creates an instance for a stream object and an inward queue
    pub fn new(
        stream: S,
        outward_queue: OutwardMessageQueue,
        inward_queue: InwardMessageQueue,
    ) -> Self {
        Self {
            stream: stream,
            reader: stream::RawMessageReader::new(BUFFER_BATCH_SIZE),
            writer: stream::RawMessageWriter::new_empty(),
            outward_queue: outward_queue,
            inward_queue: inward_queue,
        }
    }

    /// Get a copy if the outward queue to post messages to
    pub fn get_outward_queue(&self) -> OutwardMessageQueue {
        self.outward_queue.clone()
    }

    /// Get a copy if the outward queue to post messages to
    pub fn get_inward_queue(&self) -> InwardMessageQueue {
        self.inward_queue.clone()
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
                if let Err(stream::State::Empty) = self.dequeue_next_outgoing_message_tro_writer() {
                    Ok(ReadWriteStremConnectionState::Free)
                } else {
                    Ok(ReadWriteStremConnectionState::Busy)
                }
            }
            Err(stream::State::Stream(err)) => Err(err),
            Err(stream::State::Remainder) => panic!("Internal error"),
        }
    }

    /// Take message from output queue and add it to [`stream::RawMessageWriter`]
    fn dequeue_next_outgoing_message_tro_writer(&mut self) -> Result<(), stream::State> {
        match self.outward_queue.pop_outward_queue() {
            Some((message, Some(semaphore))) => Ok(self.writer =
                stream::RawMessageWriter::new_with_semaphore(
                    message,
                    BUFFER_BATCH_SIZE,
                    semaphore,
                )),
            Some((message, None)) => {
                Ok(self.writer = stream::RawMessageWriter::new(message, BUFFER_BATCH_SIZE))
            }
            None => Err(stream::State::Empty),
        }
    }

    /// Handle writing a chunk of data into [`io::Write`] with [`stream::RawMessageWriter`]
    fn proceed_sending(&mut self) -> Result<(), stream::State> {
        self.writer.write_into(&mut self.stream)
    }

    /// Handle reading a chunk of data from [`io::Read`] with [`stream::RawMessageReader`]
    fn proceed_receiving(&mut self) -> Result<(), stream::State> {
        let messages = self.reader.read_into(&mut self.stream)?;
        if !messages.is_empty() {
            self.inward_queue
                .extend_to_inward_queue(messages.into_iter())?;
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
    outward_queue: OutwardMessageQueue,
    inward_queue: InwardMessageQueue,
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
                    Ok(Ok(())) => panic!("A worker should not exit without an error condition except when it is explicitly stopped by the semaphore"),
                    Ok(error) => error,
                    Err(_) => Err(SocketInternalError::UnknownInternalError)
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
        self.outward_queue.add_to_outward_queue(message)?;
        Ok(())
    }

    pub fn send(&mut self, message: RawMessage) -> Result<(), SocketInternalError> {
        self.check_worker_state()?;
        let completion_semaphore = self.outward_queue.add_to_prio_outward_queue(message)?;
        loop {
            let (done_flag, _timeout_guard) = completion_semaphore
                .wait_timeout(std::time::Duration::from_secs(1))
                .unwrap();
            if *done_flag {
                return Ok(());
            } else if let Err(err) = self.check_worker_state() {
                return Err(err);
            }
        }
    }

    pub fn receive_async(&mut self) -> Option<RawMessage> {
        self.inward_queue.receive_async_one()
    }

    pub fn receive_async_all(&mut self) -> Vec<RawMessage> {
        self.inward_queue.receive_async_all()
    }

    pub fn receive(&mut self) -> RawMessage {
        self.inward_queue.receive_one()
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

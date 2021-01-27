use core::message::{Message, RawMessage, MessageMetadata};
use core::util;
use core::util::thread::{StoppedSemaphore, Sleeper};
use core::util::time::{LinearDurationBackoff, DurationBackoffWithDebounce};
use core::socket::{SocketInternalError};
use stream;

use std::collections::{VecDeque, HashSet};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::{Duration};
use std::cell::{RefCell};
use std::io;

const BUFFER_BATCH_SIZE: usize = 2048;

fn query_thread_default_duration_backoff() -> DurationBackoffWithDebounce<LinearDurationBackoff> {
    DurationBackoffWithDebounce::new(LinearDurationBackoff::new(
        Duration::from_millis(0),
        Duration::from_millis(500),
        20), 500000)
}

pub struct ReadWriteStreamConnectionHandle {
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
    inward_queue: Arc<Mutex<VecDeque<RawMessage>>>
}

impl ReadWriteStreamConnectionHandle {
    pub fn new(outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
               prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
               prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
               inward_queue: Arc<Mutex<VecDeque<RawMessage>>>) -> ReadWriteStreamConnectionHandle {
        Self {
            outward_queue: outward_queue,
            prio_outward_queue: prio_outward_queue,
            prio_sent_messages: prio_sent_messages,
            inward_queue: inward_queue
        }
    }

    pub fn add_to_outward_queue(&self, message: RawMessage) {
        let mut outward_queue = self.outward_queue.lock().unwrap();
        outward_queue.push_back(message)
    }

    pub fn add_to_prio_outward_queue(&self, message: RawMessage) {
        let mut prio_outward_queue = self.prio_outward_queue.lock().unwrap();
        prio_outward_queue.push_back(message)
    }

    pub fn check_prio_message_sent(&self, metadata: &MessageMetadata) -> bool {
        let mut prio_sent_messages = self.prio_sent_messages.lock().unwrap();
        prio_sent_messages.take(metadata).is_some()
    }

    pub fn receive_async_all(&self) -> Vec<RawMessage> {
        let mut inward_queue = self.inward_queue.lock().unwrap();
        inward_queue.drain(..).collect()
    }
}

#[derive(Debug)]
pub enum ReadWriteStremConnectionState {
    Busy,
    Free
}

impl ReadWriteStremConnectionState {
    pub fn is_free(&self) -> bool {
        matches!(self, ReadWriteStremConnectionState::Free)
    }
}

pub struct ReadWriteStreamConnection<S: io::Read + io::Write + Send> {
    stream: S,
    reader: stream::RawMessageReader,
    writer: stream::RawMessageWriter,
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_sent_messages: Arc<Mutex<HashSet<MessageMetadata>>>,
    inward_queue: Arc<Mutex<VecDeque<RawMessage>>>
}

impl<S: io::Read + io::Write + Send> ReadWriteStreamConnection<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream: stream,
            reader: stream::RawMessageReader::new(BUFFER_BATCH_SIZE),
            writer: stream::RawMessageWriter::new_empty(),
            prio_outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            prio_sent_messages: Arc::new(Mutex::new(HashSet::new())),
            outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            inward_queue: Arc::new(Mutex::new(VecDeque::new()))
        }
    }

    pub fn get_handle(&self) -> ReadWriteStreamConnectionHandle {
        ReadWriteStreamConnectionHandle::new(self.outward_queue.clone(),
                                             self.prio_outward_queue.clone(),
                                             self.prio_sent_messages.clone(),
                                             self.inward_queue.clone())
    }

    fn get_outward_message_from_prio_queue(&mut self) -> Option<RawMessage> {
        let mut prio_outward_queue = self.prio_outward_queue.lock().unwrap();
        prio_outward_queue.pop_front()
    }

    fn get_outward_message_from_normal_queue(&self) -> Option<RawMessage> {
        let mut outward_queue = self.outward_queue.lock().unwrap();
        outward_queue.pop_front()
    }

    pub fn start_next_message(&mut self) -> Result<(), stream::State> {
        if let Some(message) = self.get_outward_message_from_prio_queue() {
            Ok(self.writer = stream::RawMessageWriter::new(message, BUFFER_BATCH_SIZE, true))
        } else if let Some(message) = self.get_outward_message_from_normal_queue() {
            Ok(self.writer = stream::RawMessageWriter::new(message, BUFFER_BATCH_SIZE, false))
        } else {
            Err(stream::State::Empty)
        }
    }

    fn proceed_sending(&mut self) -> Result<(), stream::State> {
        match self.writer.write_into(&mut self.stream) {
            Err(stream::State::Empty) => {
                if let Some(metadata) = self.writer.take_metadata() {
                    let mut prio_sent_messages = self.prio_sent_messages.lock().unwrap();
                    prio_sent_messages.insert(metadata.clone());
                }
                Err(stream::State::Empty)
            }
            _other => _other
        }
    }

    fn proceed_receiving(&mut self) -> Result<(), stream::State> {
        let messages = self.reader.read_into(&mut self.stream)?;
        let mut inward_queue = self.inward_queue.lock().unwrap();
        messages.into_iter().for_each(|message| {
            inward_queue.push_back(message);
        });
        Ok(())
    }

    pub fn process_receiving(&mut self) -> Result<ReadWriteStremConnectionState, SocketInternalError> {
        match self.proceed_receiving() {
            Ok(()) => Ok(ReadWriteStremConnectionState::Busy),
            Err(stream::State::Remainder) => Ok(ReadWriteStremConnectionState::Busy),
            Err(stream::State::Empty) => {
                Ok(ReadWriteStremConnectionState::Free)
            }
            Err(stream::State::Stream(err)) => Err(err)
        }
    }

    pub fn process_sending(&mut self) -> Result<ReadWriteStremConnectionState, SocketInternalError> {
        match self.proceed_sending() {
            Ok(()) => Ok(ReadWriteStremConnectionState::Busy),
            Err(stream::State::Empty) => {
                if let Err(stream::State::Empty) = self.start_next_message() {
                    Ok(ReadWriteStremConnectionState::Free)
                } else {
                    Ok(ReadWriteStremConnectionState::Busy)
                }
            }
            Err(stream::State::Stream(err)) => Err(err),
            Err(stream::State::Remainder) => panic!("Internal error")
        }
    }
}

pub struct ReadWriteStreamConnectionWorker<S: io::Read + io::Write + Send> {
    stream: ReadWriteStreamConnection<S>
}

impl<S: io::Read + io::Write + Send> ReadWriteStreamConnectionWorker<S> {
    pub fn construct_from_stream(stream: ReadWriteStreamConnection<S>) -> Result<(Self, ReadWriteStreamConnectionHandle), SocketInternalError> {
        let worker = Self {
            stream: stream
        };
        let handle = worker.stream.get_handle();
        Ok((worker, handle))
    }

    pub fn main_loop(mut self, stop_semaphore: StoppedSemaphore) -> Result<(), SocketInternalError> {
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
            if stop_semaphore.is_stopped() {
                return Ok(());
            }
            sleeper.sleep();
        }
    }
}

pub struct ReadWriteStreamConnectionManager {
    handle: ReadWriteStreamConnectionHandle,
    worker_thread: RefCell<Option<std::thread::JoinHandle<Result<(), SocketInternalError>>>>,
    last_error: RefCell<Option<Result<(), SocketInternalError>>>,
    stop_semaphore: StoppedSemaphore
}

impl ReadWriteStreamConnectionManager {
    pub fn construct_from_worker_handle<S: io::Read + io::Write + Send + 'static>(stream:ReadWriteStreamConnection<S>) -> Result<Self, SocketInternalError> {
        let (worker, handle) = ReadWriteStreamConnectionWorker::construct_from_stream(stream)?;
        let stop_semaphore = StoppedSemaphore::new();
        let stop_semaphore_clone = stop_semaphore.clone();
        Ok(Self {
            handle: handle,
            worker_thread: RefCell::new(Some(thread::spawn(move || { worker.main_loop(stop_semaphore.clone()) } ))),
            last_error: RefCell::new(None),
            stop_semaphore: stop_semaphore_clone
        })
    }

    fn get_last_error(&self) -> Option<Result<(), SocketInternalError>> {
        self.last_error.borrow().clone()
    }

    fn check_worker_state(&self) -> Result<(), SocketInternalError> {
        if let Some(last_error) = self.get_last_error() {
            last_error.clone()
        } else {
            if self.stop_semaphore.is_stopped() {
                let result = match self.worker_thread.borrow_mut().take().unwrap().join() {
                    Ok(Ok(())) => panic!("A worker should not exit without an error condition except when it is explicitly stopped by the semaphore"),
                    Ok(error) => error,
                    Err(_) => Err(SocketInternalError::UnknownInternalError)
                };
                *self.last_error.borrow_mut() = Some(result.clone());
                result
            } else {
                Ok(())
            }
        }
    }

    pub fn send_async(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        self.check_worker_state()?;
        self.handle.add_to_outward_queue(message);
        Ok(())
    }

    pub fn send(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        self.check_worker_state()?;
        let metadata = message.metadata().clone();
        self.handle.add_to_prio_outward_queue(message);
        util::thread::wait_for_backoff(query_thread_default_duration_backoff(), || {
            if let Err(err) = self.check_worker_state() {
                Some(Err(err))
            } else if self.handle.check_prio_message_sent(&metadata) {
                Some(Ok(()))
            } else {
                None
            }
        })
    }

    pub fn receive_async_all(&self) -> (Vec<RawMessage>, Option<SocketInternalError>) {
        let messages = self.handle.receive_async_all();
        match self.check_worker_state() {
            Ok(()) => (messages, None),
            Err(err) => (messages, Some(err))
        }
    }
}

impl Drop for ReadWriteStreamConnectionManager {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.last_error.borrow().is_none() {
            self.stop_semaphore.stop();
            match self.worker_thread.borrow_mut().take() {
                Some(join_handle) => { join_handle.join(); },
                None => ()
            }
        }
    }
}

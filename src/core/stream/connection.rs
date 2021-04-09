use core::message::{RawMessage};
use core::util::thread::{Semaphore, Sleeper, ChgNtfMutex};
use core::util::time::{LinearDurationBackoff, DurationBackoffWithDebounce};
use core::socket::{SocketInternalError};
use core::stream;

use std::collections::{VecDeque};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::{Duration};
use std::io;

const BUFFER_BATCH_SIZE: usize = 2048;

fn query_thread_default_duration_backoff() -> DurationBackoffWithDebounce<LinearDurationBackoff> {
    DurationBackoffWithDebounce::new(LinearDurationBackoff::new(
        Duration::from_millis(0),
        Duration::from_millis(500),
        20), 500000)
}

#[derive(Clone)]
pub struct BidirectionalMessageQueue {
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<(Arc<ChgNtfMutex<bool>>, RawMessage)>>>,
    inward_queue: Arc<ChgNtfMutex<VecDeque<RawMessage>>>
}

impl BidirectionalMessageQueue {
    pub fn new() -> Self{
        Self {
            outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            prio_outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            inward_queue: ChgNtfMutex::new(VecDeque::new())
        }
    }

    pub fn add_to_outward_queue(&self, message: RawMessage) {
        let mut outward_queue = self.outward_queue.lock().unwrap();
        outward_queue.push_back(message);
    }

    pub fn add_to_prio_outward_queue(&self, message: RawMessage) -> Arc<ChgNtfMutex<bool>> {
        let mut prio_outward_queue = self.prio_outward_queue.lock().unwrap();
        let semaphore = ChgNtfMutex::new(false);
        prio_outward_queue.push_back((semaphore.clone(), message));
        semaphore
    }

    pub fn pop_outward_queue(&self) -> Option<(RawMessage, Option<Arc<ChgNtfMutex<bool>>>)> {
        if let Some((semaphore, message)) = self.prio_outward_queue.lock().unwrap().pop_front() {
            Some((message, Some(semaphore)))
        } else if let Some(message) = self.outward_queue.lock().unwrap().pop_front() {
            Some((message, None))
        } else {
            None
        }
    }

    pub fn add_to_inward_queue(&self, message: RawMessage) {
        let mut inward_queue = self.inward_queue.lock_notify().unwrap();
        inward_queue.push_back(message)
    }

    pub fn extend_to_inward_queue<T: std::iter::Iterator<Item=RawMessage>>(&self, iterator: T) {
        self.inward_queue.lock_notify().unwrap().extend(iterator);
    }

    pub fn receive_async_all(&self) -> Vec<RawMessage> {
        let mut inward_queue = self.inward_queue.lock().unwrap();
        inward_queue.drain(..).collect()
    }

    pub fn receive_async_one(&self) -> Option<RawMessage> {
        let mut inward_queue = self.inward_queue.lock().unwrap();
        inward_queue.pop_front()
    }

    pub fn receive_one_timeout(&self, timeout: Duration) -> Option<RawMessage> {
        let mut inward_queue = self.inward_queue.lock().unwrap();
        match inward_queue.pop_front() {
            Some(message) => Some(message),
            None => {
                let (mut invard_queue, _timeout_handle) = self.inward_queue.wait_timeout_on_locked(inward_queue, timeout).unwrap();
                invard_queue.pop_front()
            } 
        }
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
    queues: BidirectionalMessageQueue
}

impl<S: io::Read + io::Write + Send> ReadWriteStreamConnection<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream: stream,
            reader: stream::RawMessageReader::new(BUFFER_BATCH_SIZE),
            writer: stream::RawMessageWriter::new_empty(),
            queues: BidirectionalMessageQueue::new()
        }
    }

    pub fn get_queue(&self) -> BidirectionalMessageQueue {
        self.queues.clone()
    }

    pub fn start_next_message(&mut self) -> Result<(), stream::State> {
        match self.queues.pop_outward_queue() {
            Some((message, Some(semaphore))) => Ok(self.writer = stream::RawMessageWriter::new_with_semaphore(message, BUFFER_BATCH_SIZE, semaphore)),
            Some((message, None)) => Ok(self.writer = stream::RawMessageWriter::new(message, BUFFER_BATCH_SIZE)),
            None => Err(stream::State::Empty)
        }
    }

    fn proceed_sending(&mut self) -> Result<(), stream::State> {
        self.writer.write_into(&mut self.stream)
    }

    fn proceed_receiving(&mut self) -> Result<(), stream::State> {
        let messages = self.reader.read_into(&mut self.stream)?;
        if !messages.is_empty() {
            self.queues.extend_to_inward_queue(messages.into_iter());
        }
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
    pub fn construct_from_stream(stream: ReadWriteStreamConnection<S>) -> Result<(Self, BidirectionalMessageQueue), SocketInternalError> {
        let worker = Self {
            stream: stream
        };
        let handle = worker.stream.get_queue();
        Ok((worker, handle))
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

pub struct ReadWriteStreamConnectionManager {
    queues: BidirectionalMessageQueue,
    worker_thread: Option<std::thread::JoinHandle<Result<(), SocketInternalError>>>,
    last_error: Option<Result<(), SocketInternalError>>,
    stop_semaphore: Semaphore
}

impl ReadWriteStreamConnectionManager {
    pub fn construct_from_worker_queue<S: io::Read + io::Write + Send + 'static>(stream:ReadWriteStreamConnection<S>) -> Result<Self, SocketInternalError> {
        let (worker, queues) = ReadWriteStreamConnectionWorker::construct_from_stream(stream)?;
        let stop_semaphore = Semaphore::new();
        let stop_semaphore_clone = stop_semaphore.clone();
        Ok(Self {
            queues: queues,
            worker_thread: Some(thread::spawn(move || { worker.main_loop(stop_semaphore.clone()) } )),
            last_error: None,
            stop_semaphore: stop_semaphore_clone
        })
    }


    fn check_worker_state(&mut self) -> Result<(), SocketInternalError> {
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
        self.queues.add_to_outward_queue(message);
        Ok(())
    }

    pub fn send(&mut self, message: RawMessage) -> Result<(), SocketInternalError> {
        self.check_worker_state()?;
        let completion_semaphore = self.queues.add_to_prio_outward_queue(message);
        loop {
            let (done_flag, _timeout_guard) = completion_semaphore.wait_timeout(std::time::Duration::from_secs(1)).unwrap();
            if *done_flag {
                return Ok(())
            } else if let Err(err) = self.check_worker_state() {
                return Err(err)
            }
        }
    }

    pub fn receive_async_all(&mut self) -> (Vec<RawMessage>, Option<SocketInternalError>) {
        let messages = self.queues.receive_async_all();
        match self.check_worker_state() {
            Ok(()) => (messages, None),
            Err(err) => (messages, Some(err))
        }
    }
}

impl Drop for ReadWriteStreamConnectionManager {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.last_error.is_none() {
            self.stop_semaphore.signal();
            match self.worker_thread.take() {
                Some(join_handle) => { join_handle.join(); },
                None => ()
            }
        }
    }
}

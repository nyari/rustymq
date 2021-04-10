use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use core::message::{Message, PeerId, RawMessage};
use core::util::thread::ChgNtfMutex;

#[allow(dead_code)]
#[derive(Debug)]
pub enum QueueOverflowHandling {
    Throttle,
    Drop,
    Error,
    Panic,
}
#[allow(dead_code)]
pub struct QueueingPolicy {
    queue_depth: Option<(QueueOverflowHandling, usize)>,
}

#[derive(Clone)]
pub struct OutwardMessageQueue {
    outward_queue: Arc<Mutex<VecDeque<RawMessage>>>,
    prio_outward_queue: Arc<Mutex<VecDeque<(Arc<ChgNtfMutex<bool>>, RawMessage)>>>,
}

impl OutwardMessageQueue {
    pub fn new() -> Self {
        Self {
            outward_queue: Arc::new(Mutex::new(VecDeque::new())),
            prio_outward_queue: Arc::new(Mutex::new(VecDeque::new())),
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
}

#[derive(Clone)]
pub struct InwardMessageQueue {
    inward_queue: Arc<ChgNtfMutex<VecDeque<RawMessage>>>,
}

impl InwardMessageQueue {
    pub fn new() -> Self {
        Self {
            inward_queue: ChgNtfMutex::new(VecDeque::new()),
        }
    }

    pub fn new_with_peer_side(peer_id: PeerId) -> (Self, InwardMessageQueuePeerSide) {
        let queue = Self::new();
        (
            queue.clone(),
            InwardMessageQueuePeerSide::new(queue, peer_id),
        )
    }

    pub fn create_peer_side_queue(&self, peer_id: PeerId) -> InwardMessageQueuePeerSide {
        InwardMessageQueuePeerSide::new(self.clone(), peer_id)
    }

    pub fn add_to_inward_queue(&self, message: RawMessage) {
        let mut inward_queue = self.inward_queue.lock_notify().unwrap();
        inward_queue.push_back(message)
    }

    pub fn extend_to_inward_queue<T: std::iter::Iterator<Item = RawMessage>>(&self, iterator: T) {
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
                let (mut invard_queue, _timeout_handle) = self
                    .inward_queue
                    .wait_timeout_on_locked(inward_queue, timeout)
                    .unwrap();
                invard_queue.pop_front()
            }
        }
    }
}

pub struct InwardMessageQueuePeerSide {
    queue: InwardMessageQueue,
    peer_id: PeerId,
}

impl InwardMessageQueuePeerSide {
    pub fn new(queue: InwardMessageQueue, peer_id: PeerId) -> Self {
        Self {
            queue: queue,
            peer_id: peer_id,
        }
    }

    pub fn add_to_inward_queue(&self, message: RawMessage) {
        self.queue
            .add_to_inward_queue(message.apply_peer_id(self.peer_id))
    }

    pub fn extend_to_inward_queue<T: std::iter::Iterator<Item = RawMessage>>(&self, iterator: T) {
        self.queue
            .extend_to_inward_queue(iterator.map(|message| message.apply_peer_id(self.peer_id)))
    }
}

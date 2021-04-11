use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use core::socket::{SocketInternalError};
use core::message::{Message, PeerId, RawMessage};
use core::util::thread::{ChgNtfMutex, ChgNtfMutexGuard};

#[derive(Debug, Clone)]
pub enum QueueOverflowHandling {
    Throttle,
    Drop,
    Error,
    Panic,
}

pub type QueueingPolicy = (QueueOverflowHandling, usize);

struct QueuePolicyEnforcer<'a, T>
where
    T: Send + Sync {
    queue: &'a ChgNtfMutex<VecDeque<T>>,
    policy: &'a Option<QueueingPolicy>
}

impl<'a, T> QueuePolicyEnforcer<'a, T>
where
    T: Send + Sync {
    pub fn new(queue: &'a ChgNtfMutex<VecDeque<T>>, policy: &'a Option<QueueingPolicy>) -> Self {
        Self {
            queue: queue,
            policy: policy
        }
    }

    pub fn push_front(&'a self, value: T) -> Result<(), SocketInternalError> {
        Ok(())
    }

    pub fn push_back(&'a self, value: T) -> Result<(), SocketInternalError> {
        Ok(())
    }

    pub fn extend<I: std::iter::Iterator<Item = T>>(&'a self, iter: I) -> Result<(), SocketInternalError> {
        Ok(())
    }

    pub fn pop_front(&'a self) -> Option<T> {
        None
    }

    pub fn pop_all(&'a self) -> Vec<T> {
        Vec::new()
    }
 }

#[derive(Clone)]
pub struct OutwardMessageQueue {
    outward_queue: Arc<ChgNtfMutex<VecDeque<(RawMessage, Option<Arc<ChgNtfMutex<bool>>>)>>>,
    policy: Option<QueueingPolicy>,
}

impl OutwardMessageQueue {
    pub fn new() -> Self {
        Self {
            outward_queue: Arc::new(ChgNtfMutex::new(VecDeque::new())),
            policy: None
        }
    }

    pub fn add_to_outward_queue(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        QueuePolicyEnforcer::new(&self.outward_queue, &self.policy).push_back((message, None))
    }

    pub fn add_to_prio_outward_queue(&self, message: RawMessage) -> Result<Arc<ChgNtfMutex<bool>>, SocketInternalError> {
        let semaphore = ChgNtfMutex::new_arc(false);
        QueuePolicyEnforcer::new(&self.outward_queue, &self.policy).push_back((message, Some(semaphore.clone())))?;
        Ok(semaphore)
    }

    pub fn pop_outward_queue(&self) -> Option<(RawMessage, Option<Arc<ChgNtfMutex<bool>>>)> {
        QueuePolicyEnforcer::new(&self.outward_queue, &self.policy).pop_front()
    }
}

#[derive(Clone)]
pub struct InwardMessageQueue {
    inward_queue: Arc<ChgNtfMutex<VecDeque<RawMessage>>>,
    policy: Option<QueueingPolicy>,
}

impl InwardMessageQueue {
    pub fn new() -> Self {
        Self {
            inward_queue: ChgNtfMutex::new_arc(VecDeque::new()),
            policy: None
        }
    }

    pub fn with_policy(self, policy: Option<QueueingPolicy>) -> Self {
        Self {
            policy: policy,
            ..self
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

    pub fn add_to_inward_queue(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        QueuePolicyEnforcer::new(&self.inward_queue, &self.policy).push_back(message)
    }

    pub fn extend_to_inward_queue<T: std::iter::Iterator<Item = RawMessage>>(&self, iterator: T) -> Result<(), SocketInternalError> {
        QueuePolicyEnforcer::new(&self.inward_queue, &self.policy).extend(iterator)
    }

    pub fn receive_async_all(&self) -> Vec<RawMessage> {
        QueuePolicyEnforcer::new(&self.inward_queue, &self.policy).pop_all()
    }

    pub fn receive_async_one(&self) -> Option<RawMessage> {
        QueuePolicyEnforcer::new(&self.inward_queue, &self.policy).pop_front()
    }

    pub fn receive_one_timeout(&self, timeout: Duration) -> Option<RawMessage> {
        let mut inward_queue = self.inward_queue.lock_notify().unwrap();
        match inward_queue.pop_front() {
            Some(message) => Some(message),
            None => {
                let (mut invard_queue, _timeout_handle) = self
                    .inward_queue
                    .wait_timeout_on_lock_notified(inward_queue, timeout)
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

    pub fn add_to_inward_queue(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        self.queue
            .add_to_inward_queue(message.apply_peer_id(self.peer_id))
    }

    pub fn extend_to_inward_queue<T: std::iter::Iterator<Item = RawMessage>>(&self, iterator: T) -> Result<(), SocketInternalError> {
        self.queue
            .extend_to_inward_queue(iterator.map(|message| message.apply_peer_id(self.peer_id)))
    }
}

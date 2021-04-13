use std::sync::Arc;
use std::time::Duration;
use std::{collections::VecDeque, ops::DerefMut};

use core::message::{RawMessage};
use core::socket::SocketInternalError;
use core::util::thread::ChgNtfMutex;

#[derive(Debug, Clone)]
pub enum QueueOverflowHandling {
    Throttle,
    Drop,
    ErrorAndDrop,
    ErrorAndForceExtend,
    Panic,
}

pub type QueueingPolicy = (QueueOverflowHandling, usize);

struct QueuePolicyEnforcer<'a, T>
where
    T: Send + Sync,
{
    queue: &'a ChgNtfMutex<VecDeque<T>>,
    policy: &'a Option<QueueingPolicy>,
}

impl<'a, T> QueuePolicyEnforcer<'a, T>
where
    T: Send + Sync,
{
    pub fn new(queue: &'a ChgNtfMutex<VecDeque<T>>, policy: &'a Option<QueueingPolicy>) -> Self {
        Self {
            queue: queue,
            policy: policy,
        }
    }

    fn grow_queue<F: FnOnce(&mut VecDeque<T>)>(
        &self,
        modifier: F,
    ) -> Result<(), SocketInternalError> {
        match self.policy {
            None => Ok(modifier(self.queue.lock_notify().unwrap().deref_mut())),
            Some((QueueOverflowHandling::Throttle, queue_depth)) => {
                let mut queue = self.queue.lock_notify().unwrap();
                while queue.len() >= *queue_depth {
                    queue = self
                        .queue
                        .wait_timeout_on_lock_notified(queue, std::time::Duration::from_secs(1))
                        .unwrap()
                        .0;
                }
                Ok(modifier(queue.deref_mut()))
            }
            Some((QueueOverflowHandling::Drop, queue_depth)) => {
                let mut queue = self.queue.lock_notify().unwrap();
                if queue.len() < *queue_depth {
                    modifier(queue.deref_mut());
                }
                Ok(())
            }
            Some((QueueOverflowHandling::ErrorAndDrop, queue_depth)) => {
                let mut queue = self.queue.lock_notify().unwrap();
                if queue.len() < *queue_depth {
                    modifier(queue.deref_mut());
                    Ok(())
                } else {
                    Err(SocketInternalError::QueueDepthReached)
                }
            }
            Some((QueueOverflowHandling::ErrorAndForceExtend, queue_depth)) => {
                let mut queue = self.queue.lock_notify().unwrap();
                modifier(queue.deref_mut());
                if queue.len() <= *queue_depth {
                    Ok(())
                } else {
                    Err(SocketInternalError::QueueDepthReached)
                }
            }
            Some((QueueOverflowHandling::Panic, queue_depth)) => {
                let mut queue = self.queue.lock_notify().unwrap();
                if queue.len() >= *queue_depth {
                    panic!("Queue full");
                }
                Ok(modifier(queue.deref_mut()))
            }
        }
    }

    pub fn push_front(&'a self, value: T) -> Result<(), SocketInternalError> {
        self.grow_queue(|queue| queue.push_front(value))
    }

    pub fn push_back(&'a self, value: T) -> Result<(), SocketInternalError> {
        self.grow_queue(|queue| queue.push_back(value))
    }

    pub fn extend<I: std::iter::Iterator<Item = T>>(
        &'a self,
        iter: I,
    ) -> Result<(), SocketInternalError> {
        self.grow_queue(|queue| queue.extend(iter))
    }

    pub fn pop_front(&'a self) -> Option<T> {
        self.queue.lock_notify().unwrap().pop_front()
    }

    pub fn pop_all(&'a self) -> Vec<T> {
        self.queue.lock_notify().unwrap().drain(..).collect()
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
            policy: None,
        }
    }

    pub fn with_policy(self, policy: Option<QueueingPolicy>) -> Self {
        Self {
            policy: policy,
            ..self
        }
    }

    pub fn add_to_outward_queue(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        QueuePolicyEnforcer::new(&self.outward_queue, &self.policy).push_back((message, None))
    }

    pub fn add_to_prio_outward_queue(
        &self,
        message: RawMessage,
    ) -> Result<Arc<ChgNtfMutex<bool>>, SocketInternalError> {
        let semaphore = ChgNtfMutex::new_arc(false);
        QueuePolicyEnforcer::new(&self.outward_queue, &self.policy)
            .push_front((message, Some(semaphore.clone())))?;
        Ok(semaphore)
    }

    pub fn pop_outward_queue(&self) -> Option<(RawMessage, Option<Arc<ChgNtfMutex<bool>>>)> {
        QueuePolicyEnforcer::new(&self.outward_queue, &self.policy).pop_front()
    }
}

#[derive(Clone)]
pub struct InwardMessageQueueNotifier {
    signal: Arc<ChgNtfMutex<bool>>
}

impl InwardMessageQueueNotifier {
    pub fn new() -> Self {
        Self {
            signal: Arc::new(ChgNtfMutex::new(false))
        }
    }

    pub fn notify_change(&self) {
        *self.signal.lock_notify().unwrap() = true;
    }

    pub fn wait_for_change_timeout(&self, timeout: std::time::Duration) -> bool {
        let mut signal = self.signal.lock().unwrap();
        let result = if *signal == true {
            true
        } else {
            signal = self.signal.wait_timeout_on_locked(signal, timeout).unwrap().0;
            if *signal == true {
                true
            } else {
                false
            }
        };
        *signal = false;
        result
    }

    pub fn wait_for_change(&self) -> bool {
        let mut signal = self.signal.lock().unwrap();
        if !*signal == true {
            while !*signal {
                signal = self.signal.wait_on_locked(signal).unwrap();
            }
        };
        *signal = false;
        true
    }

    pub fn take_was_change(&self) -> bool {
        let mut signal = self.signal.lock().unwrap();
        let result = *signal;
        *signal = false;
        result
    }
}

#[derive(Clone)]
pub struct InwardMessageQueue {
    inward_queue: Arc<ChgNtfMutex<VecDeque<RawMessage>>>,
    policy: Option<QueueingPolicy>,
    notifier: InwardMessageQueueNotifier
}

impl InwardMessageQueue {
    pub fn new(notifier: InwardMessageQueueNotifier) -> Self {
        Self {
            inward_queue: ChgNtfMutex::new_arc(VecDeque::new()),
            policy: None,
            notifier: notifier
        }
    }

    pub fn with_policy(self, policy: Option<QueueingPolicy>) -> Self {
        Self {
            policy: policy,
            ..self
        }
    }

    pub fn add_to_inward_queue(&self, message: RawMessage) -> Result<(), SocketInternalError> {
        let result = QueuePolicyEnforcer::new(&self.inward_queue, &self.policy).push_back(message);
        self.notifier.notify_change();
        result
    }

    pub fn extend_to_inward_queue<T: std::iter::Iterator<Item = RawMessage>>(
        &self,
        iterator: T,
    ) -> Result<(), SocketInternalError> {
        let result = QueuePolicyEnforcer::new(&self.inward_queue, &self.policy).extend(iterator);
        self.notifier.notify_change();
        result
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

    pub fn receive_one(&self) -> RawMessage {
        let mut inward_queue = self.inward_queue.lock_notify().unwrap();
        match inward_queue.pop_front() {
            Some(message) => message,
            None => {
                while inward_queue.is_empty() {  
                    inward_queue = self
                        .inward_queue
                        .wait_on_lock_notified(inward_queue)
                        .unwrap();
                }
                inward_queue.pop_front().unwrap()
            }
        }
    }
}

// pub struct InwardMessageQueuePeerSide {
//     queue: InwardMessageQueue,
//     peer_id: PeerId,
// }

// impl InwardMessageQueuePeerSide {
//     pub fn new(queue: InwardMessageQueue, peer_id: PeerId) -> Self {
//         Self {
//             queue: queue,
//             peer_id: peer_id,
//         }
//     }

//     pub fn add_to_inward_queue(&self, message: RawMessage) -> Result<(), SocketInternalError> {
//         self.queue
//             .add_to_inward_queue(message.apply_peer_id(self.peer_id))
//     }

//     pub fn extend_to_inward_queue<T: std::iter::Iterator<Item = RawMessage>>(
//         &self,
//         iterator: T,
//     ) -> Result<(), SocketInternalError> {
//         self.queue
//             .extend_to_inward_queue(iterator.map(|message| message.apply_peer_id(self.peer_id)))
//     }
// }

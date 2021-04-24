use std::sync::Arc;
use std::time::Duration;
use std::{collections::VecDeque, ops::DerefMut};
use std::ops::{Deref, Drop};

use core::message::{RawMessage};
use core::socket::SocketInternalError;
use core::util::thread::ChgNtfMutex;


#[derive(Debug, Clone)]
pub enum MessageQueueOverflowHandling {
    Throttle,
    Drop,
    ErrorAndDrop,
    ErrorAndForceExtend,
    Panic,
}

pub enum MessageQueueError {
    SendersAllDropped,
    ReceiversAllDropped,
    Timeout
}

pub type MessageQueueingPolicy = (MessageQueueOverflowHandling, usize);

#[derive(Copy, Clone)]
pub enum ReceiptState {
    /// The message is still in queue
    InQueue,
    /// The message has been received but **Processed** state will be provided once message is processed
    Received,
    /// The message has been received but the signaling of **Processed** state will not happen 
    Acknowledged,
    /// The message has been processed
    Processed,
    /// The message did not arrive and will not arrive in future because there are no receivers, or queue has been dropped.
    Dropped
}

impl ReceiptState {
    pub fn in_queue(&self) -> Result<bool, Self> {
        match self {
            ReceiptState::InQueue => Ok(true),
            ReceiptState::Dropped => Err(*self),
            _ => Ok(false)
        }
    }

    pub fn has_been_received(&self) -> Result<bool, Self> {
        match self {
            ReceiptState::Received | ReceiptState::Acknowledged | ReceiptState::Processed => Ok(true),
            ReceiptState::InQueue => Ok(false),
            _ => return Err(*self),
        }
    }

    pub fn has_been_processed(&self) -> Result<bool, Self> {
        match self {
            ReceiptState::Processed => Ok(true),
            ReceiptState::InQueue | ReceiptState::Received => Ok(false),
            _ => return Err(*self),
        }
    }

    fn transition(&mut self, state: Self) {
        let new_state: Result<Self, ()> = match (&self, state) {
            (Self::InQueue, Self::Received) => Ok(state),
            (Self::InQueue, Self::Acknowledged) => Ok(state),
            (Self::Received, Self::Processed) => Ok(state),
            (_, Self::Dropped) => Ok(state),
            _ => Err(())
        };

        *self = new_state.expect("Invalid ReceiptState transition");
    }
}

#[derive(Clone)]
struct ReceiptInternalData(pub Arc<ChgNtfMutex<ReceiptState>>);

impl ReceiptInternalData {
    pub fn queue() -> Self{ 
        Self(Arc::new(ChgNtfMutex::new(ReceiptState::InQueue)))
    }

    pub fn sender_receipt(&self) -> SenderReceipt {
        SenderReceipt::new(self.clone())
    }

    pub fn receiver_receipt(&self) -> ReceiverReceipt {
        ReceiverReceipt::new(self.clone())
    }

    pub fn acnkowledged(self) {
        self.0.lock_notify().unwrap().transition(ReceiptState::Acknowledged);
    }
}

impl Drop for ReceiptInternalData {
    fn drop(&mut self) {
        self.0.lock_notify().unwrap().transition(ReceiptState::Dropped);
    }
}

pub struct SenderReceipt(ReceiptInternalData);

impl SenderReceipt {
    fn new(internals: ReceiptInternalData) -> Self {
        Self(internals)
    } 

    pub fn state(&self) -> ReceiptState {
        *self.0.0.lock().unwrap()
    }

    pub fn wait_received(&self) -> Result<ReceiptState, ReceiptState> {
        let mut locked = self.0.0.lock().unwrap();
        loop {
            locked = self.0.0.wait_on_locked(locked).unwrap();
            if let true = locked.has_been_received()? {
                return Ok(*locked);
            } 
        }
    }

    pub fn wait_received_timeout(&self, timeout: Duration) -> Result<ReceiptState, ReceiptState> {
        let mut locked = self.0.0.lock().unwrap();
        if let true = locked.has_been_received()? {
            Ok(*locked)
        } else {
            locked = self.0.0.wait_on_locked(locked).unwrap();
            if let true = locked.deref().has_been_received()? {
                Ok(*locked)
            } else {
                Ok(*locked)
            }
        }
    }

    pub fn wait_processed(&self) -> Result<ReceiptState, ReceiptState> {
        let mut locked = self.0.0.lock().unwrap();
        loop {
            locked = self.0.0.wait_on_locked(locked).unwrap();
            if let true = locked.has_been_processed()? {
                return Ok(*locked);
            } 
        }
    }

    pub fn wait_processed_timeout(&self, timeout: Duration) -> Result<ReceiptState, ReceiptState> {
        let mut locked = self.0.0.lock().unwrap();
        if let true = locked.has_been_processed()? {
            Ok(*locked)
        } else {
            locked = self.0.0.wait_on_locked(locked).unwrap();
            if let true = locked.deref().has_been_processed()? {
                Ok(*locked)
            } else {
                Ok(*locked)
            }
        }
    }
}

pub struct ReceiverReceipt(ReceiptInternalData);

impl ReceiverReceipt {
    fn new(internals: ReceiptInternalData) -> Self {
        internals.0.lock_notify().unwrap().transition(ReceiptState::Received);
        Self(internals)
    }

    fn processed(self) {
        self.0.0.lock_notify().unwrap().transition(ReceiptState::Processed);
    }
}

struct MessageQueueInternalData<T> 
    where T: Send + Sync {
    pub queue: VecDeque<(Option<ReceiptInternalData>, T)>,
    pub receiver_count: usize,
    pub sender_count: usize,
    pub policy: MessageQueueingPolicy
}

impl<T> MessageQueueInternalData<T>
    where T: Send + Sync {
    fn new(policy: MessageQueueingPolicy) -> Self {
        Self {
            queue: VecDeque::new(),
            receiver_count: 0,
            sender_count: 0,
            policy: policy
        }
    }

    fn has_receiver(&self) -> bool {
        self.receiver_count > 0
    }

    fn has_sender(&self) -> bool {
        self.sender_count > 0
    }
}

struct MessageQueueInternal<T>(Arc<ChgNtfMutex<MessageQueueInternalData<T>>>)
    where T: Send + Sync;

impl<T> MessageQueueInternal<T>
    where T: Send + Sync {
    fn new(policy: MessageQueueingPolicy) -> Self {
        Self(ChgNtfMutex::new_arc(MessageQueueInternalData::new(policy)))
    }

    fn receive(&self) -> Result<T, MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();
        loop {
            match locked.queue.pop_front() {
                Some((receipt, message)) => {
                    receipt.map(|x| x.acnkowledged());
                    return Ok(message)
                },
                None => {
                    if !locked.has_sender() {
                        return Err(MessageQueueError::SendersAllDropped)
                    }
                }
            }
            locked = self.0.wait_on_lock_notified(locked).unwrap()
        }
    }

    fn receive_timeout(&self, timeout: Duration) -> Result<T, MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();
        for _ in 0..2 {
            match locked.queue.pop_front() {
                Some((receipt, message)) => {
                    receipt.map(|x| x.acnkowledged());
                    return Ok(message)
                },
                None => {
                    if !locked.has_sender() {
                        return Err(MessageQueueError::SendersAllDropped)
                    }
                }
            }
            locked = self.0.wait_timeout_on_lock_notified(locked, timeout).unwrap().0
        }

        Err(MessageQueueError::Timeout)
    }

    fn receive_async(&self) -> Result<Option<T>, MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();
        match locked.queue.pop_front() {
            Some((receipt, message)) => {
                receipt.map(|x| x.acnkowledged());
                Ok(Some(message))
            },
            None => {
                if !locked.has_sender() {
                    Err(MessageQueueError::SendersAllDropped)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn receive_all(&self) -> Result<Vec<T>, MessageQueueError> {
        let result: Vec<T> = self.0.lock_notify()
                                   .unwrap()
                                   .queue
                                   .drain(..)
                                   .map(|(receipt, message)| {
                                       receipt.map(|x| x.acnkowledged());
                                       message
                                   })
                                   .collect();

        if !result.is_empty() {
            Ok(result)
        } else if !self.0.lock().unwrap().has_sender() {
            Err(MessageQueueError::SendersAllDropped)
        } else {
            Ok(result)
        }
    }

    fn receive_all_with_receipt(&self) -> Result<Vec<(Option<ReceiverReceipt>, T)>, MessageQueueError> {
        let result:Vec<(Option<ReceiverReceipt>, T)> = self.0.lock_notify()
                                                             .unwrap()
                                                             .queue
                                                             .drain(..)
                                                             .map(|(receipt, message)| {
                                                                  match receipt {
                                                                      Some(r) => (Some(ReceiverReceipt::new(r)), message),
                                                                      None => (None, message)
                                                                  }
                                                             })
                                                             .collect();
        if !result.is_empty() {
            Ok(result)
        } else if !self.0.lock().unwrap().has_sender() {
            Err(MessageQueueError::SendersAllDropped)
        } else {
            Ok(result)
        }
    }
}
//-----------------------------------------------------------------------------------------------------------------------------------------

pub type QueueingPolicy = (QueueOverflowHandling, usize);

#[derive(Debug, Clone)]
pub enum QueueOverflowHandling {
    Throttle,
    Drop,
    ErrorAndDrop,
    ErrorAndForceExtend,
    Panic,
}

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

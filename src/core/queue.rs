use std::sync::Arc;
use std::time::Duration;
use std::collections::VecDeque;
use std::ops::{Deref, Drop};

use core::util::thread::ChgNtfMutex;

#[derive(Debug)]
pub enum MessageQueueError {
    SendersAllDropped,
    ReceiversAllDropped,
    Timeout,
    QueueFull,
    Dropped
}

#[derive(Debug, Clone)]
pub enum MessageQueueOverflowHandling {
    Throttle,
    Drop,
    ErrorAndDrop,
    ErrorAndForceExtend,
    Panic,
}

#[derive(Clone, Debug)]
pub struct MessageQueueingPolicy {
    pub overflow: Option<(MessageQueueOverflowHandling, usize)>
}

impl MessageQueueingPolicy {
    pub fn default() -> Self {
        Self {
            overflow: None
        }
    }

    pub fn with_overflow(self, overflow: Option<(MessageQueueOverflowHandling, usize)>) -> Self {
        Self {
            overflow: overflow,
            ..self
        }
    }
}

#[derive(Copy, Clone, Debug)]
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
            (Self::Acknowledged, Self::Dropped) => Ok(Self::Acknowledged),
            (Self::Processed, Self::Dropped) => Ok(Self::Processed),
            (_, Self::Dropped) => Ok(state),
            _ => Err(())
        };

        *self = new_state.expect(format!("Invalid ReceiptState transition. From: {:?}, To: {:?}", self, state).as_str());
    }
}

#[derive(Clone)]
struct ReceiptInternalData(pub Arc<ChgNtfMutex<ReceiptState>>);

impl ReceiptInternalData {
    fn queue() -> (SenderReceipt, ReceiverReceipt) { 
        let internals = Self(Arc::new(ChgNtfMutex::new(ReceiptState::InQueue)));
        (SenderReceipt::new(internals.clone()), ReceiverReceipt::new(internals))
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
            locked = self.0.0.wait_timeout_on_locked(locked, timeout).unwrap().0;
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
            if let true = locked.has_been_processed()? {
                return Ok(*locked);
            }
            locked = self.0.0.wait_on_locked(locked).unwrap();
        }
    }

    pub fn wait_processed_timeout(&self, timeout: Duration) -> Result<ReceiptState, ReceiptState> {
        let mut locked = self.0.0.lock().unwrap();
        if let true = locked.has_been_processed()? {
            Ok(*locked)
        } else {
            locked = self.0.0.wait_timeout_on_locked(locked, timeout).unwrap().0;
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
        Self(internals)
    }

    pub fn received(self) -> Self {
        self.0.0.lock_notify().unwrap().transition(ReceiptState::Received);
        self
    }

    pub fn acnkowledged(self) {
        self.0.0.lock_notify().unwrap().transition(ReceiptState::Acknowledged);
    }

    pub fn processed(self) {
        self.0.0.lock_notify().unwrap().transition(ReceiptState::Processed);
    }
}

impl Drop for ReceiverReceipt {
    fn drop(&mut self) {
        self.0.0.lock_notify().unwrap().transition(ReceiptState::Dropped);
    }
}

struct MessageQueueCounter {
    receiver_count: Option<usize>,
    sender_count: Option<usize>,
}

impl MessageQueueCounter {
    pub fn new() -> Self {
        Self {
            receiver_count: Some(0),
            sender_count: Some(0)
        }
    }

    fn increment_counter(count: &mut Option<usize>) {
        *count = match count.as_ref() {
            Some(value) => Some(value + 1 as usize),
            None => Some(1)
        };
    }

    fn decrement_counter(count: &mut Option<usize>) {
        *count = match count.as_ref() {
            Some(value) => if *value > 1 {
                Some(value - 1 as usize)
            } else if *value == 1 {
                None
            } else {
                panic!("Counter cannot be decremented")
            },
            None => panic!("Counter cannot be decremented")
        };
    }

    pub fn increment_receiver(&mut self) {
        Self::increment_counter(&mut self.receiver_count)
    }

    pub fn increment_sender(&mut self) {
        Self::increment_counter(&mut self.sender_count)
    }

    pub fn decrement_receiver(&mut self) {
        Self::decrement_counter(&mut self.receiver_count)
    }

    pub fn decrement_sender(&mut self) {
        Self::decrement_counter(&mut self.sender_count)
    }

    pub fn sender_count(&self) -> usize {
        self.sender_count.unwrap_or(0)
    }

    pub fn senders_dropped(&self) -> bool {
        self.sender_count.is_none()
    }

    pub fn receivers_dropped(&self) -> bool {
        self.receiver_count.is_none()
    }
}

struct MessageQueueInternalData<T> 
    where T: Send + Sync {
    pub queue: VecDeque<(Option<ReceiverReceipt>, T)>,
    pub counters: MessageQueueCounter,
    pub policy: MessageQueueingPolicy
}

impl<T> MessageQueueInternalData<T>
    where T: Send + Sync {
    fn new(policy: MessageQueueingPolicy) -> Self {
        Self {
            queue: VecDeque::new(),
            counters: MessageQueueCounter::new(),
            policy: policy
        }
    }

    fn receivers_all_dropped(&self) -> bool {
        self.counters.receivers_dropped()
    }

    fn senders_all_dropped(&self) -> bool {
        self.counters.senders_dropped()
    }

    fn is_queue_full(&self) -> bool {
        match self.policy.overflow {
            Some((_, limit)) => limit <= self.queue.len() * self.counters.sender_count(),
            None => false
        }
    }

    fn get_overflow_handling(&self) -> Option<MessageQueueOverflowHandling> {
        self.policy.overflow.clone().map(|x| x.0)
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
                    if locked.senders_all_dropped() {
                        return Err(MessageQueueError::SendersAllDropped)
                    }
                }
            }
            locked = self.0.wait_on_lock_notified(locked).unwrap()
        }
    }

    fn receive_with_receipt(&self) -> Result<(Option<ReceiverReceipt>, T), MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();
        loop {
            match locked.queue.pop_front() {
                Some((receipt, message)) => {
                    return Ok((receipt.map(|x| x.received()), message))
                },
                None => {
                    if locked.senders_all_dropped() {
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
                    if locked.senders_all_dropped() {
                        return Err(MessageQueueError::SendersAllDropped)
                    }
                }
            }
            locked = self.0.wait_timeout_on_lock_notified(locked, timeout).unwrap().0
        }

        Err(MessageQueueError::Timeout)
    }

    fn receive_timeout_with_receipt(&self, timeout: Duration) -> Result<(Option<ReceiverReceipt>, T), MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();
        for _ in 0..2 {
            match locked.queue.pop_front() {
                Some((receipt, message)) => {
                    return Ok((receipt.map(|x| x.received()), message))
                },
                None => {
                    if locked.senders_all_dropped() {
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
                if locked.senders_all_dropped() {
                    Err(MessageQueueError::SendersAllDropped)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn receive_async_with_receipt(&self) -> Result<Option<(Option<ReceiverReceipt>, T)>, MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();
        match locked.queue.pop_front() {
            Some((receipt, message)) => {
                Ok(Some((receipt.map(|x| x.received()), message)))
            },
            None => {
                if locked.senders_all_dropped() {
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
        } else if self.0.lock().unwrap().senders_all_dropped() {
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
                                                                      Some(r) => (Some(r.received()), message),
                                                                      None => (None, message)
                                                                  }
                                                             })
                                                             .collect();
        if !result.is_empty() {
            Ok(result)
        } else if self.0.lock().unwrap().senders_all_dropped() {
            Err(MessageQueueError::SendersAllDropped)
        } else {
            Ok(result)
        }
    }

    fn send(&self, message: T) -> Result<(), MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();

        loop {
            if locked.receivers_all_dropped() {
                return Err(MessageQueueError::ReceiversAllDropped)
            }

            if !locked.is_queue_full() {
                locked.queue.push_back((None, message));
                return Ok(());
            } else {
                match locked.get_overflow_handling() {
                    Some(MessageQueueOverflowHandling::Throttle) => (),
                    Some(MessageQueueOverflowHandling::Drop) => return Ok(()),
                    Some(MessageQueueOverflowHandling::ErrorAndDrop) => return Err(MessageQueueError::QueueFull),
                    Some(MessageQueueOverflowHandling::ErrorAndForceExtend) => {
                        locked.queue.push_back((None, message));
                        return Err(MessageQueueError::QueueFull);
                    },
                    Some(MessageQueueOverflowHandling::Panic) => panic!("MessageQueue full!"),
                    _ => panic!("Internal error, impossible case happened")
                }
            }
            locked = self.0.wait_on_lock_notified(locked).unwrap();
        }
    }

    fn send_unless_full(&self, message: T) -> Result<(), (MessageQueueError, T)> {
        let mut locked = self.0.lock_notify().unwrap();
        if locked.receivers_all_dropped() {
            return Err((MessageQueueError::ReceiversAllDropped, message))
        }
        if !locked.is_queue_full() {
            locked.queue.push_back((None, message));
            Ok(())
        } else {
            Err((MessageQueueError::QueueFull, message))
        }
    }

    fn send_with_receipt(&self, message: T) -> Result<SenderReceipt, MessageQueueError> {
        let mut locked = self.0.lock_notify().unwrap();

        let (sender_receipt, receiver_receipt) = ReceiptInternalData::queue();
        loop {
            if locked.receivers_all_dropped() {
                return Err(MessageQueueError::ReceiversAllDropped)
            }

            if !locked.is_queue_full() {
                locked.queue.push_back((Some(receiver_receipt), message));
                return Ok(sender_receipt);
            } else {
                match locked.get_overflow_handling() {
                    Some(MessageQueueOverflowHandling::Throttle) => (),
                    Some(MessageQueueOverflowHandling::Drop) => return Err(MessageQueueError::Dropped),
                    Some(MessageQueueOverflowHandling::ErrorAndDrop) => return Err(MessageQueueError::QueueFull),
                    Some(MessageQueueOverflowHandling::ErrorAndForceExtend) => {
                        locked.queue.push_back((Some(receiver_receipt), message));
                        return Err(MessageQueueError::QueueFull);
                    },
                    Some(MessageQueueOverflowHandling::Panic) => panic!("MessageQueue full!"),
                    _ => panic!("Internal error, impossible case happened")
                }
            }
            locked = self.0.wait_on_lock_notified(locked).unwrap();
        }
    }

    fn send_with_receipt_unless_full(&self, message: T) -> Result<SenderReceipt, (MessageQueueError, T)> {
        let mut locked = self.0.lock_notify().unwrap();

        let (sender_receipt, receiver_receipt) = ReceiptInternalData::queue();
        if locked.receivers_all_dropped() {
            return Err((MessageQueueError::ReceiversAllDropped, message))
        }

        if !locked.is_queue_full() {
            locked.queue.push_back((Some(receiver_receipt), message));
            Ok(sender_receipt)
        } else {
            Err((MessageQueueError::QueueFull, message))
        }
    }
}

impl<T> Clone for MessageQueueInternal<T>
    where T: Send + Sync {

    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}


#[derive(Clone)]
pub struct MessageQueueReceiver<T>(MessageQueueInternal<T>) where T: Send + Sync;

impl<T> MessageQueueReceiver<T>
    where T: Send + Sync {
    pub fn new(policy: MessageQueueingPolicy) -> Self {
        Self::new_internals(MessageQueueInternal::new(policy))
    }

    fn new_internals(internals: MessageQueueInternal<T>) -> Self {
        internals.0.lock_notify().unwrap().counters.increment_receiver();
        Self(internals)
    }

    pub fn sender_pair(&self) -> MessageQueueSender<T> {
        MessageQueueSender::new_internals(self.0.clone())
    }

    pub fn receive(&self) -> Result<T, MessageQueueError> {
        self.0.receive()
    }

    pub fn receive_with_receipt(&self) -> Result<(Option<ReceiverReceipt>, T), MessageQueueError> {
        self.0.receive_with_receipt()
    }

    pub fn receive_timeout(&self, timeout: Duration) -> Result<T, MessageQueueError> {
        self.0.receive_timeout(timeout)
    }

    pub fn receive_timeout_with_receipt(&self, timeout: Duration) -> Result<(Option<ReceiverReceipt>, T), MessageQueueError> {
        self.0.receive_timeout_with_receipt(timeout)
    }

    pub fn receive_async(&self) -> Result<Option<T>, MessageQueueError> {
        self.0.receive_async()
    }

    pub fn receive_async_with_receipt(&self) -> Result<Option<(Option<ReceiverReceipt>, T)>, MessageQueueError> {
        self.0.receive_async_with_receipt()
    }

    pub fn receive_all(&self) -> Result<Vec<T>, MessageQueueError> {
        self.0.receive_all()
    }

    pub fn receive_all_with_receipt(&self) -> Result<Vec<(Option<ReceiverReceipt>, T)>, MessageQueueError> {
        self.0.receive_all_with_receipt()
    }
}

impl<T> Drop for MessageQueueReceiver<T>
    where T: Send + Sync
{
    fn drop(&mut self) {
        self.0.0.lock_notify().unwrap().counters.decrement_receiver();
    }
}

#[derive(Clone)]
pub struct MessageQueueSender<T>(MessageQueueInternal<T>) where T: Send + Sync;

impl<T> MessageQueueSender<T>
    where T: Send + Sync {
    pub fn new(policy: MessageQueueingPolicy) -> Self {
        Self::new_internals(MessageQueueInternal::new(policy))
    }

    fn new_internals(internals: MessageQueueInternal<T>) -> Self {
        internals.0.lock_notify().unwrap().counters.increment_sender();
        Self(internals)
    }

    pub fn receiver_pair(&self) -> MessageQueueReceiver<T> {
        MessageQueueReceiver::new_internals(self.0.clone())
    }

    pub fn send(&self, message: T) -> Result<(), MessageQueueError> {
        self.0.send(message)
    }

    pub fn send_unless_full(&self, message: T) -> Result<(), (MessageQueueError, T)> {
        self.0.send_unless_full(message)
    }

    pub fn send_with_receipt(&self, message: T) -> Result<SenderReceipt, MessageQueueError> {
        self.0.send_with_receipt(message)
    }

    pub fn send_with_receipt_unless_full(&self, message: T) -> Result<SenderReceipt, (MessageQueueError, T)> {
        self.0.send_with_receipt_unless_full(message)
    }
}

impl<T> Drop for MessageQueueSender<T>
    where T: Send + Sync
{
    fn drop(&mut self) {
        self.0.0.lock_notify().unwrap().counters.decrement_sender();
    }
}
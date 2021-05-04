//! # Queueing base module
//! Contains datastructures for handling queues in RustyMQ

/// Error handling of internal message queues
#[derive(Debug)]
pub enum MessageQueueError {
    SendersAllDropped,
    ReceiversAllDropped,
    Timeout,
    QueueFull,
    Dropped,
}

/// Configuration on how to handle queue overflow errors
#[derive(Debug, Clone)]
pub enum MessageQueueOverflowHandling {
    Throttle,
    Drop,
    ErrorAndDrop,
    ErrorAndForceExtend,
    Panic,
}

/// Message queueing policy setting
#[derive(Clone, Debug)]
pub struct MessageQueueingPolicy {
    /// Overflow handling setting with depth of queue
    pub overflow: Option<(MessageQueueOverflowHandling, usize)>,
}

impl MessageQueueingPolicy {
    /// Create the default queueing policy
    pub fn default() -> Self {
        Self { overflow: None }
    }

    /// Apply owerflow handling into the policy
    pub fn with_overflow(self, overflow: Option<(MessageQueueOverflowHandling, usize)>) -> Self {
        Self {
            overflow: overflow,
            ..self
        }
    }
}

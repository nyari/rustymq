#[derive(Debug)]
pub enum MessageQueueError {
    SendersAllDropped,
    ReceiversAllDropped,
    Timeout,
    QueueFull,
    Dropped,
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
    pub overflow: Option<(MessageQueueOverflowHandling, usize)>,
}

impl MessageQueueingPolicy {
    pub fn default() -> Self {
        Self { overflow: None }
    }

    pub fn with_overflow(self, overflow: Option<(MessageQueueOverflowHandling, usize)>) -> Self {
        Self {
            overflow: overflow,
            ..self
        }
    }
}

use std::any::{Any};

use core::queue::{QueueingPolicy};

#[derive(Debug)]
pub struct TransportConfiguration {
    pub queue_policy: Option<QueueingPolicy>,
    pub extra: Option<Box<Any>>,
}

impl TransportConfiguration {
    pub fn new() -> Self {
        Self {
            queue_policy: None,
            extra: None
        }
    }

    pub fn with_queue_policy(self, policy: Option<QueueingPolicy>) -> Self {
        Self {
            queue_policy: policy,
            ..self
        }
    }

    pub fn with_extra(self, extra: Option<Box<Any>>) -> Self {
        Self {
            extra: extra,
            ..self
        }
    }
}
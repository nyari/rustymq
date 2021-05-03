use std::any::Any;
use std::sync::Arc;

use crate::base::queue::MessageQueueingPolicy;

#[derive(Debug, Clone)]
pub struct TransportConfiguration {
    pub queue_policy: MessageQueueingPolicy,
    pub extra: Option<Arc<dyn Any + Send + Sync>>,
}

impl TransportConfiguration {
    pub fn new() -> Self {
        Self {
            queue_policy: MessageQueueingPolicy::default(),
            extra: None,
        }
    }

    pub fn with_queue_policy(self, policy: MessageQueueingPolicy) -> Self {
        Self {
            queue_policy: policy,
            ..self
        }
    }

    pub fn with_extra(self, extra: Option<Arc<dyn Any + Send + Sync>>) -> Self {
        Self {
            extra: extra,
            ..self
        }
    }
}

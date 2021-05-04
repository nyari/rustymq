//! # Configuration base module
//! Contains data structures and utilites to configure and fine tune the operation of `Socket`s and `Transport`s

use std::any::Any;
use std::sync::Arc;

use crate::base::queue::MessageQueueingPolicy;

/// Used for the configuration of [`crate::base::Transport`]s
#[derive(Debug, Clone)]
pub struct TransportConfiguration {
    /// Set up the queueing policy
    pub queue_policy: MessageQueueingPolicy,
    /// Configuration for custom `Transport` implementations
    pub custom: Option<Arc<dyn Any + Send + Sync>>,
}

impl TransportConfiguration {
    /// Create new configuration
    pub fn new() -> Self {
        Self {
            queue_policy: MessageQueueingPolicy::default(),
            custom: None,
        }
    }

    /// Apply queue policy onto the configuration
    pub fn with_queue_policy(self, policy: MessageQueueingPolicy) -> Self {
        Self {
            queue_policy: policy,
            ..self
        }
    }

    /// Apply custom configuration for custom `Transport`s
    pub fn with_custom(self, custom: Option<Arc<dyn Any + Send + Sync>>) -> Self {
        Self {
            custom: custom,
            ..self
        }
    }
}

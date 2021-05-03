/// # Timing related helpers
/// Timing functionality used internally by RustyMQ

use std::time::Duration;

/// # DurationBackoff interface
/// Trait to implement a backoff algorithm
pub trait DurationBackoff: Clone {
    /// Step backoff algorithm to next state
    fn step(&mut self) -> Duration;
    /// Get current state of backoff algorithm
    fn current(&self) -> Duration;
    /// Reset backoff algorithm to default state
    fn reset(&mut self) -> Duration;
}

/// # LinearDurationBackoff
/// Implementation of a linear backoff algorithm that linearly increases the backoff duration
/// between two states
#[derive(Clone)]
pub struct LinearDurationBackoff {
    low: Duration,
    high: Duration,
    step: Duration,
    state: Duration,
}

impl LinearDurationBackoff {
    /// Create an instance that backs of between `low` and `high` in `steps` number of steps
    pub fn new(low: Duration, high: Duration, steps: u16) -> Self {
        Self {
            low: low,
            high: high,
            step: (high - low) / steps as u32,
            state: low,
        }
    }
}

impl DurationBackoff for LinearDurationBackoff {
    #[inline]
    fn step(&mut self) -> Duration {
        let result = self.state;
        if self.state < self.high {
            self.state += self.step
        }
        result
    }

    #[inline]
    fn current(&self) -> Duration {
        self.state
    }

    #[inline]
    fn reset(&mut self) -> Duration {
        self.state = self.low;
        self.state
    }
}

/// # DurationBackoffWithDebounce
/// Duration backup implementation that debounces a given number of steps before handing over to the internal
/// backoff algorithm
#[derive(Clone)]
pub struct DurationBackoffWithDebounce<T: DurationBackoff> {
    backoff: T,
    debounce: usize,
    state: usize,
}

impl<T: DurationBackoff> DurationBackoffWithDebounce<T> {
    /// Create new instance with `backoff` algorithm with `debounce` number of debouncing before it
    pub fn new(backoff: T, debounce: usize) -> Self {
        Self {
            backoff: backoff,
            debounce: debounce,
            state: 0,
        }
    }
}

impl<T: DurationBackoff> DurationBackoff for DurationBackoffWithDebounce<T> {
    #[inline]
    fn step(&mut self) -> Duration {
        if self.state < self.debounce {
            self.state += 1;
            Duration::new(0, 0)
        } else {
            self.backoff.step()
        }
    }

    #[inline]
    fn current(&self) -> Duration {
        if self.state < self.debounce {
            Duration::new(0, 0)
        } else {
            self.backoff.current()
        }
    }

    #[inline]
    fn reset(&mut self) -> Duration {
        self.state = 0;
        self.backoff.reset();
        Duration::new(0, 0)
    }
}
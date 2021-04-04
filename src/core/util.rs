//! # Core internal utilites
//! The module contains miscellaneous datastructures and functionalities
//! used by RustyMQ

use rand;
use core::serializer::{Serializable, Serializer, Deserializer};
use core::serializer;
use std;

/// # Identifier
/// Used as an identifier in messages as peer and other identifiers
#[derive(PartialEq)]
#[derive(Eq)]
#[derive(PartialOrd)]
#[derive(Ord)]
#[derive(Hash)]
#[derive(Clone)]
#[derive(Copy)]
#[derive(Debug)]
pub struct Identifier
{
    core: u64  
}

impl Identifier {
    /// Create a new identifier from seed
    pub fn new(core: u64) -> Self {
        Self {
            core:core
        }
    }

    /// Create a random identifier
    pub fn new_random() -> Self {
        Self {
            core: rand::random()
        }
    }

    /// Query internal state of identifier
    pub fn get(&self) -> u64 {
        self.core.clone()
    }
}

pub struct VersionInfo {
    main: u16,
    sub: u16,
    rev: u16
}

impl VersionInfo {
    pub fn current() -> Self {
        Self {
            main: 0,
            sub: 0,
            rev: 0
        }
    }

    pub fn new(main: u16, sub: u16, rev: u16) -> Self {
        Self {
            main: main,
            sub: sub,
            rev: rev
        }
    }

    pub fn get(&self) -> (u16, u16, u16) {
        (self.main, self.sub, self.rev)
    }

    pub fn interface_compatible(&self, other: &Self) -> bool {
        self.main >= other.main
    }

    pub fn feature_compatible(&self, other: &Self) -> bool {
        self.main >= other.main
    }
}

impl Serializable for Identifier {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.core);
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(
            deserializer.deserialize()?
        ))
    }
}

impl Serializable for VersionInfo {
    fn serialize<T:Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.main);
        serializer.serialize(&self.sub);
        serializer.serialize(&self.rev);
    }
    fn deserialize<T:Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?
        ))
    }
}

/// Iterate through single element
pub struct SingleIter<T> {
    item: Option<T>
}

impl<T> SingleIter<T> {
    pub fn new(item: T) -> Self {
        Self {
            item: Some(item)
        }
    }
}

impl<T> std::iter::Iterator for SingleIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.item.take()
    }
}
/// # Timing related helpers
/// Timing functionality used internally by RustyMQ
pub mod time {
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
        state: Duration
    }

    impl LinearDurationBackoff {
        /// Create an instance that backs of between `low` and `high` in `steps` number of steps
        pub fn new(low: Duration, high: Duration, steps: u16) -> Self {
            Self {
                low: low,
                high: high,
                step: (high - low) / steps as u32,
                state: low
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
        state: usize
    }

    impl<T: DurationBackoff> DurationBackoffWithDebounce<T> {
        /// Create new instance with `backoff` algorithm with `debounce` number of debouncing before it
        pub fn new(backoff: T, debounce: usize) -> Self {
            Self {
                backoff: backoff,
                debounce: debounce,
                state: 0
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
}

/// # Threaded operation helper functionality
/// Timing functionality used internally by RustyMQ
pub mod thread {
    use std::sync::{Arc, Mutex};
    use std::thread;

    /// #Sleeper
    /// A helper struct that allows for sleeping the currrent thread according to a duration backoff algorithm
    pub struct Sleeper<T:super::time::DurationBackoff> {
        backoff: T
    }

    impl<T:super::time::DurationBackoff> Sleeper<T> {
        /// Construct new sleeper with `backoff` algorithm
        pub fn new(backoff: T) -> Self {
            Self {
                backoff: backoff
            }
        }

        /// Sleep the current thread according to the next state of the backoff algorithm
        pub fn sleep(&mut self) {
            thread::sleep(self.backoff.step())
        }

        /// Reset the internal backoff algorithm
        pub fn reset(&mut self) {
            self.backoff.reset();
        }
    }

    /// Execute `operation` repeadetly until success with `sleep_time` sleep between attemps
    pub fn poll<T, F: Fn() -> Option<T>>(sleep_time: std::time::Duration, operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(sleep_time)
        }
    }

    /// Execute `operation` repeadetly until success with `sleep_time` sleep between attemps
    pub fn poll_mut<T, F: FnMut() -> Option<T>>(sleep_time: std::time::Duration, mut operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(sleep_time)
        }
    }

    /// Execute `operation` repeadetly until success with a [`Sleeper`] sleep between each attempt
    pub fn poll_backoff<T, B: super::time::DurationBackoff, F: Fn() -> Option<T>>(backoff: B, operation: F) -> T {
        let mut sleeper = Sleeper::new(backoff);
        loop {
            if let Some(result) = operation() {
                return result
            }
            sleeper.sleep()
        }
    }

    /// Execute `operation` repeadetly until success with a [`Sleeper`] sleep between each attempt
    pub fn poll_backoff_mut<T, B: super::time::DurationBackoff, F: FnMut() -> Option<T>>(backoff: B, mut operation: F) -> T {
        let mut sleeper = Sleeper::new(backoff);
        loop {
            if let Some(result) = operation() {
                return result
            }
            sleeper.sleep()
        }
    }

    /// # StoppedSemaphore
    /// Simple semaphore to signal if a thread has stopped
    #[derive(Clone)]
    pub struct StoppedSemaphore {
        semaphore: Arc<Mutex<bool>>
    }

    impl StoppedSemaphore {
        /// Create semaphore
        pub fn new() -> Self {
            Self {
                semaphore: Arc::new(Mutex::new(false))
            }
        }
        
        /// Check if semaphore has been stopped
        pub fn is_stopped(&self) -> bool {
            if let Ok(value) = self.semaphore.lock() {
                *value
            } else {
                true
            }
        }

        /// Explicity signal stopped state of semaphore
        pub fn stop(&self) {
            if let Ok(mut value) = self.semaphore.lock() {
                *value = true
            }
        }
    }

    impl Drop for StoppedSemaphore {
        fn drop(&mut self) {
            self.stop()
        }
    }
}
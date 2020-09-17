use rand;
use core::serializer::{Serializable, Serializer, Deserializer};
use core::serializer;

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
    pub fn new(core: u64) -> Self {
        Self {
            core:core
        }
    }

    pub fn new_random() -> Self {
        Self {
            core: rand::random()
        }
    }

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

pub mod time {
    use std::time::Duration;

    pub trait DurationBackoff: Clone {

        fn step(&mut self) -> Duration;
        fn current(&self) -> Duration;
        fn reset(&mut self) -> Duration;
    }

    #[derive(Clone)]
    pub struct LinearDurationBackoff {
        low: Duration,
        high: Duration,
        step: Duration,
        state: Duration
    }

    impl LinearDurationBackoff {
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

    #[derive(Clone)]
    pub struct DurationBackoffWithDebounce<T: DurationBackoff> {
        backoff: T,
        debounce: usize,
        state: usize
    }

    impl<T: DurationBackoff> DurationBackoffWithDebounce<T> {
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

pub mod thread {
    use std::sync::{Arc, Mutex};

    pub fn wait_for<T, F: Fn() -> Option<T>>(sleep_time: std::time::Duration, operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(sleep_time)
        }
    }

    pub fn wait_for_mut<T, F: FnMut() -> Option<T>>(sleep_time: std::time::Duration, mut operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(sleep_time)
        }
    }

    pub fn wait_for_backoff<T, B: super::time::DurationBackoff, F: Fn() -> Option<T>>(mut backoff: B, operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(backoff.step())
        }
    }

    pub fn wait_for_backoff_mut<T, B: super::time::DurationBackoff, F: FnMut() -> Option<T>>(mut backoff: B, mut operation: F) -> T {
        loop {
            if let Some(result) = operation() {
                return result
            }
            std::thread::sleep(backoff.step())
        }
    }

    #[derive(Clone)]
    pub struct StoppedSemaphore {
        semaphore: Arc<Mutex<bool>>
    }

    impl StoppedSemaphore {
        pub fn new() -> Self {
            Self {
                semaphore: Arc::new(Mutex::new(false))
            }
        }

        pub fn is_stopped(&self) -> bool {
            if let Ok(value) = self.semaphore.lock() {
                *value
            } else {
                true
            }
        }

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
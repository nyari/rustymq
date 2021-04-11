//! # Core internal utilites
//! The module contains miscellaneous datastructures and functionalities
//! used by RustyMQ

use core::serializer;
use core::serializer::{Deserializer, Serializable, Serializer};
use rand;
use std;

/// # Identifier
/// Used as an identifier in messages as peer and other identifiers
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Debug)]
pub struct Identifier {
    core: u64,
}

impl Identifier {
    /// Create a new identifier from seed
    pub fn new(core: u64) -> Self {
        Self { core: core }
    }

    /// Create a random identifier
    pub fn new_random() -> Self {
        Self {
            core: rand::random(),
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
    rev: u16,
}

impl VersionInfo {
    pub fn current() -> Self {
        Self {
            main: 0,
            sub: 0,
            rev: 0,
        }
    }

    pub fn new(main: u16, sub: u16, rev: u16) -> Self {
        Self {
            main: main,
            sub: sub,
            rev: rev,
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
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.core);
    }
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(deserializer.deserialize()?))
    }
}

impl Serializable for VersionInfo {
    fn serialize<T: Serializer>(&self, serializer: &mut T) {
        serializer.serialize(&self.main);
        serializer.serialize(&self.sub);
        serializer.serialize(&self.rev);
    }
    fn deserialize<T: Deserializer>(deserializer: &mut T) -> Result<Self, serializer::Error> {
        Ok(Self::new(
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?,
            deserializer.deserialize::<u16>()?,
        ))
    }
}

/// Iterate through single element
pub struct SingleIter<T> {
    item: Option<T>,
}

impl<T> SingleIter<T> {
    pub fn new(item: T) -> Self {
        Self { item: Some(item) }
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
}

/// # Threaded operation helper functionality
/// Timing functionality used internally by RustyMQ
pub mod thread {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, LockResult, Mutex, MutexGuard, PoisonError, WaitTimeoutResult};

    #[inline]
    pub fn sleep(sleep_time: std::time::Duration) {
        if sleep_time.subsec_nanos() != 0 || sleep_time.as_secs() != 0 {
            std::thread::sleep(sleep_time)
        }
    }

    /// #Sleeper
    /// A helper struct that allows for sleeping the currrent thread according to a duration backoff algorithm
    pub struct Sleeper<T: super::time::DurationBackoff> {
        backoff: T,
    }

    impl<T: super::time::DurationBackoff> Sleeper<T> {
        /// Construct new sleeper with `backoff` algorithm
        pub fn new(backoff: T) -> Self {
            Self { backoff: backoff }
        }

        /// Sleep the current thread according to the next state of the backoff algorithm
        pub fn sleep(&mut self) {
            sleep(self.backoff.step())
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
                return result;
            }
            sleep(sleep_time)
        }
    }

    /// Execute `operation` repeadetly until success with `sleep_time` sleep between attemps
    pub fn poll_mut<T, F: FnMut() -> Option<T>>(
        sleep_time: std::time::Duration,
        mut operation: F,
    ) -> T {
        loop {
            if let Some(result) = operation() {
                return result;
            }
            sleep(sleep_time)
        }
    }

    /// Execute `operation` repeadetly until success with a [`Sleeper`] sleep between each attempt
    pub fn poll_backoff<T, B: super::time::DurationBackoff, F: Fn() -> Option<T>>(
        backoff: B,
        operation: F,
    ) -> T {
        let mut sleeper = Sleeper::new(backoff);
        loop {
            if let Some(result) = operation() {
                return result;
            }
            sleeper.sleep()
        }
    }

    /// Execute `operation` repeadetly until success with a [`Sleeper`] sleep between each attempt
    pub fn poll_backoff_mut<T, B: super::time::DurationBackoff, F: FnMut() -> Option<T>>(
        backoff: B,
        mut operation: F,
    ) -> T {
        let mut sleeper = Sleeper::new(backoff);
        loop {
            if let Some(result) = operation() {
                return result;
            }
            sleeper.sleep()
        }
    }

    /// # Semaphore
    /// Simple semaphore to signal if a thread has stopped
    #[derive(Clone)]
    pub struct Semaphore {
        semaphore: Arc<AtomicBool>,
    }

    impl Semaphore {
        /// Create semaphore
        pub fn new() -> Self {
            Self {
                semaphore: Arc::new(AtomicBool::from(false)),
            }
        }

        /// Check if semaphore has been stopped
        pub fn is_signaled(&self) -> bool {
            self.semaphore.load(Ordering::Relaxed)
        }

        /// Explicity signal stopped state of semaphore
        pub fn signal(&self) {
            self.semaphore.store(true, Ordering::Relaxed);
        }
    }

    impl Drop for Semaphore {
        fn drop(&mut self) {
            self.signal()
        }
    }

    pub struct ChangeNotifyMutexGuard<'a, T>
    where
        T: Send + Sync,
    {
        internal_mutex_guard: Option<MutexGuard<'a, T>>,
        mutex: &'a ChangeNotifyMutex<T>,
    }

    impl<'a, T> ChangeNotifyMutexGuard<'a, T>
    where
        T: Send + Sync,
    {
        pub fn new(
            mutex: &'a ChangeNotifyMutex<T>,
        ) -> Result<Self, PoisonError<MutexGuard<'a, T>>> {
            let internal_mutex_guard = mutex.lock()?;
            Ok(Self::with_mutex_ref(mutex, internal_mutex_guard))
        }

        pub fn with_mutex_ref(
            mutex: &'a ChangeNotifyMutex<T>,
            mutex_ref: MutexGuard<'a, T>,
        ) -> Self {
            Self {
                internal_mutex_guard: Some(mutex_ref),
                mutex: mutex,
            }
        }

        pub fn into_internal_guard(mut self) -> MutexGuard<'a, T> {
            self.internal_mutex_guard.take().unwrap()
        }
    }

    impl<'a, T> Drop for ChangeNotifyMutexGuard<'a, T>
    where
        T: Send + Sync,
    {
        fn drop(&mut self) {
            #[allow(unused_must_use)]
            {
                if match self.internal_mutex_guard.take() {
                    Some(_mutex) => true,
                    None => false,
                } {
                    self.mutex.notify_all();
                }
            }
        }
    }

    impl<'a, T> std::ops::Deref for ChangeNotifyMutexGuard<'a, T>
    where
        T: Send + Sync,
    {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            self.internal_mutex_guard.as_ref().unwrap().deref()
        }
    }

    impl<'a, T> std::ops::DerefMut for ChangeNotifyMutexGuard<'a, T>
    where
        T: Send + Sync,
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.internal_mutex_guard.as_mut().unwrap().deref_mut()
        }
    }

    pub struct ChangeNotifyMutex<T>
    where
        T: Send + Sync,
    {
        value: Mutex<T>,
        var: Condvar,
    }

    pub type ChgNtfMutex<T> = ChangeNotifyMutex<T>;
    pub type ChgNtfMutexGuard<'a, T> = ChangeNotifyMutexGuard<'a, T>;

    impl<T> ChangeNotifyMutex<T>
    where
        T: Send + Sync,
    {
        pub fn new(value: T) -> Self {
            Self {
                value: Mutex::new(value),
                var: Condvar::new(),
            }
        }

        pub fn new_arc(value: T) -> Arc<Self> {
            Arc::new(Self::new(value))
        }

        pub fn lock<'a>(&'a self) -> LockResult<MutexGuard<'a, T>> {
            self.value.lock()
        }

        pub fn lock_notify<'a>(
            &'a self,
        ) -> Result<ChangeNotifyMutexGuard<'a, T>, PoisonError<MutexGuard<'a, T>>> {
            ChangeNotifyMutexGuard::new(&self)
        }

        pub fn wait<'a>(&'a self) -> LockResult<MutexGuard<'a, T>> {
            self.wait_on_locked(self.value.lock()?)
        }

        pub fn wait_on_locked<'a>(
            &'a self,
            guard: MutexGuard<'a, T>,
        ) -> LockResult<MutexGuard<'a, T>> {
            self.var.wait(guard)
        }

        pub fn wait_on_lock_notified<'a>(
            &'a self,
            guard: ChangeNotifyMutexGuard<'a, T>,
        ) -> LockResult<ChangeNotifyMutexGuard<'a, T>> {
            let wait_result = self.var.wait(guard.into_internal_guard());
            match wait_result {
                Ok(mutex) => Ok(ChangeNotifyMutexGuard::with_mutex_ref(self, mutex)),
                Err(poison_error) => Err(PoisonError::new({
                    ChangeNotifyMutexGuard::with_mutex_ref(self, poison_error.into_inner())
                })),
            }
        }

        pub fn wait_while<'a, F: for<'r> FnMut(&'r mut T) -> bool>(
            &'a self,
            condition: F,
        ) -> LockResult<MutexGuard<'a, T>> {
            self.wait_while_on_locked(self.value.lock()?, condition)
        }

        pub fn wait_while_on_locked<'a, F: for<'r> FnMut(&'r mut T) -> bool>(
            &'a self,
            guard: MutexGuard<'a, T>,
            condition: F,
        ) -> LockResult<MutexGuard<'a, T>> {
            self.var.wait_while(guard, condition)
        }

        pub fn wait_timeout<'a>(
            &'a self,
            timeout: std::time::Duration,
        ) -> LockResult<(MutexGuard<'a, T>, Option<WaitTimeoutResult>)> {
            self.wait_timeout_on_locked(
                match self.value.lock() {
                    Ok(guard) => Ok(guard),
                    Err(poison_error) => Err(PoisonError::new((poison_error.into_inner(), None))),
                }?,
                timeout,
            )
        }

        pub fn wait_timeout_on_locked<'a>(
            &'a self,
            mutex_guard: MutexGuard<'a, T>,
            timeout: std::time::Duration,
        ) -> LockResult<(MutexGuard<'a, T>, Option<WaitTimeoutResult>)> {
            match self.var.wait_timeout(mutex_guard, timeout) {
                Ok((guard, timeout)) => Ok((guard, Some(timeout))),
                Err(poison_error) => {
                    let (guard, timeout) = poison_error.into_inner();
                    Err(PoisonError::new((guard, Some(timeout))))
                }
            }
        }

        pub fn wait_timeout_on_lock_notified<'a>(
            &'a self,
            guard: ChangeNotifyMutexGuard<'a, T>,
            timeout: std::time::Duration,
        ) -> LockResult<(ChangeNotifyMutexGuard<'a, T>, WaitTimeoutResult)> {
            let wait_result = self.var.wait_timeout(guard.into_internal_guard(), timeout);
            match wait_result {
                Ok((mutex, timeout)) => {
                    Ok((ChangeNotifyMutexGuard::with_mutex_ref(self, mutex), timeout))
                }
                Err(poison_error) => Err(PoisonError::new({
                    let (mutex, timeout) = poison_error.into_inner();
                    (ChangeNotifyMutexGuard::with_mutex_ref(self, mutex), timeout)
                })),
            }
        }

        pub fn wait_timeout_while<'a, F: for<'r> FnMut(&'r mut T) -> bool>(
            &'a self,
            timeout: std::time::Duration,
            condition: F,
        ) -> LockResult<(MutexGuard<'a, T>, Option<WaitTimeoutResult>)> {
            self.wait_timeout_while_on_locked(
                match self.value.lock() {
                    Ok(guard) => Ok(guard),
                    Err(poison_error) => Err(PoisonError::new((poison_error.into_inner(), None))),
                }?,
                timeout,
                condition,
            )
        }

        pub fn wait_timeout_while_on_locked<'a, F: for<'r> FnMut(&'r mut T) -> bool>(
            &'a self,
            guard: MutexGuard<'a, T>,
            timeout: std::time::Duration,
            condition: F,
        ) -> LockResult<(MutexGuard<'a, T>, Option<WaitTimeoutResult>)> {
            match self.var.wait_timeout_while(guard, timeout, condition) {
                Ok((guard, timeout)) => Ok((guard, Some(timeout))),
                Err(poison_error) => {
                    let (guard, timeout) = poison_error.into_inner();
                    Err(PoisonError::new((guard, Some(timeout))))
                }
            }
        }

        pub fn notify_one(&self) {
            self.var.notify_one();
        }

        pub fn notify_all(&self) {
            self.var.notify_all();
        }

        pub fn change_and_notify_all<'a, U, F: Fn(MutexGuard<'a, T>) -> U>(
            &'a self,
            operation: F,
        ) -> Result<U, LockResult<MutexGuard<'a, T>>> {
            let result = match self.value.lock() {
                Ok(guard) => Ok(operation(guard)),
                Err(err) => Err(Err(err)),
            };
            self.var.notify_all();
            result
        }

        pub fn change_mut_and_notify_all<'a, U, F: FnMut(MutexGuard<'a, T>) -> U>(
            &'a self,
            mut operation: F,
        ) -> Result<U, LockResult<MutexGuard<'a, T>>> {
            let result = match self.value.lock() {
                Ok(guard) => Ok(operation(guard)),
                Err(err) => Err(Err(err)),
            };
            self.var.notify_all();
            result
        }
    }
}

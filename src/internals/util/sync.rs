//! # Thread synchronization helper utilites

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LockResult, Mutex, MutexGuard, PoisonError, WaitTimeoutResult};

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
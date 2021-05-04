/// # Multi-threaded operation helper functionality
/// Timing functionality used internally by RustyMQ

/// Sleep the current thread. In case of duration == 0, then no sleep
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
    pub fn sleep(&mut self) -> std::time::Duration {
        let dur = self.backoff.step();
        sleep(dur);
        dur
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
        sleeper.sleep();
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
        sleeper.sleep();
    }
}
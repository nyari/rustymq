use lazy_static::{lazy_static};
use std::time;
use std::ops::Deref;

lazy_static! {
    /// Base instant at the start of the application. Used as a reference timer from start of application.
    /// This is needed in order to measure times
    pub static ref BASE_INSTANT: time::Instant = time::Instant::now();
}

/// Custom Duration datastructure wrapping [`std::time::Duration`] to make in more convenient to handle four our purposes of
/// measuring the message round trip time
#[derive(Debug)]
pub struct Duration(pub time::Duration);

impl Duration {
    /// Create a new instance with [`std::time::Duration`]
    pub fn new(duration: time::Duration) -> Self {
        Self(duration)
    }

    /// Create a new instance relative to the time the application was started
    pub fn now() -> Self {
        Self::new(BASE_INSTANT.elapsed())
    }
}

/// Make it convenient to handle the stored [`std::time::Duration`]
impl Deref for Duration {
    type Target = time::Duration;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}



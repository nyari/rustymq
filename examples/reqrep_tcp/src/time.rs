use lazy_static::{lazy_static};
use std::time;
use std::ops::Deref;

lazy_static! {
    pub static ref BASE_INSTANT: time::Instant = time::Instant::now();
}

#[derive(Debug)]
pub struct Duration(pub time::Duration);

impl Duration {
    pub fn new(duration: time::Duration) -> Self {
        Self(duration)
    }

    pub fn now() -> Self {
        Self::new(BASE_INSTANT.elapsed())
    }
}

impl Deref for Duration {
    type Target = time::Duration;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}



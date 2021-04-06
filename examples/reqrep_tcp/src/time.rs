use lazy_static::{lazy_static};
use std::time::{Instant};

lazy_static! {
    static ref BASE_INSTANT: Instant = Instant::now();
}


//! # Internals module
//! Internal functionality of RustyMQ that is not needed for the usage of the library but is required for
//! development or extension, customizaton of the library

pub mod queue;
pub mod serializer;
pub mod socket;
pub mod stream;

#[allow(dead_code)]
pub mod util;

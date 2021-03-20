pub mod tcp;
#[cfg(feature="network-openssl-socket-support")]
pub mod ssl;

mod internal;
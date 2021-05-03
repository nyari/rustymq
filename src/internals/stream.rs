//! # Stream module
//! Helper functionality for implementing [`crate::core::transport::Transport`]s for [`std::io::Read`]
//! and [`std::io::Write`] capable streams

mod connection;
mod header;
mod info;
mod reader;
mod tracker;
mod writer;

pub use self::connection::{ReadWriteStreamConnection, ReadWriteStreamConnectionThreadManager};
pub use self::info::State;
pub use self::reader::StreamSerializableReader;
pub use self::writer::StreamSerializableWriter;

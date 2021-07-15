//! # Internal utilites
//! The module contains miscellaneous datastructures and functionalities
//! used by RustyMQ

pub mod sync;
pub mod thread;
pub mod time;

use std;
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

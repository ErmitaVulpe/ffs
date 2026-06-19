use std::marker::PhantomData;

use crate::db::DEFAULT_CHUNK_SIZE;

pub struct Splitter<Behavior> {
    bytes_left: u64,
    goal_size: u32,
    _marker: PhantomData<Behavior>,
}

/// Simple splitting behavior. Adds as many max size chunks as possible
pub struct Simple;

impl Splitter<Simple> {
    pub fn new(len: u64) -> Self {
        Self {
            bytes_left: len,
            goal_size: DEFAULT_CHUNK_SIZE,
            _marker: PhantomData,
        }
    }
}

impl<T> Iterator for Splitter<T> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes_left > self.goal_size as u64 {
            self.bytes_left -= self.goal_size as u64;
            Some(self.goal_size)
        } else if self.bytes_left > 0 {
            let remainder = self.bytes_left as u32;
            self.bytes_left = 0;
            Some(remainder)
        } else {
            None
        }
    }
}

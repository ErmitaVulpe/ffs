use std::collections::{HashMap, hash_map::Entry};

use derive_more::Constructor;
use weighted_rs::{SmoothWeight, Weight};

use crate::{db::BackendId, splitter::DEFAULT_CHUNK_SIZE};

pub struct ChunkAlloc {
    stat: HashMap<BackendId, BackendStat>,
    weights: SmoothWeight<BackendId>,
}

impl ChunkAlloc {
    pub fn new(stat: HashMap<BackendId, BackendStat>) -> Result<Self, redb::Error> {
        let weights = SmoothWeight::new();
        let mut round_robin = Self { stat, weights };
        round_robin.recalibrate();
        Ok(round_robin)
    }

    pub fn recalibrate(&mut self) {
        self.weights.remove_all();
        for (k, v) in &self.stat {
            if v.free < DEFAULT_CHUNK_SIZE as u64 {
                continue;
            }
            self.weights.add(*k, v.free as isize);
        }
    }

    /// Return None when out of space
    pub fn allocate(&mut self, chunk_size: u32) -> Option<BackendId> {
        let id = self.weights.next()?;
        match self.stat.entry(id) {
            Entry::Vacant(_) => unreachable!(),
            Entry::Occupied(mut entry) => {
                let stat = entry.get_mut();
                let new_free = stat.free.checked_sub(chunk_size as u64)?;
                stat.free = new_free;
                if new_free < DEFAULT_CHUNK_SIZE as u64 {
                    self.recalibrate();
                }
            }
        }

        Some(id)
    }
}

#[derive(Debug, Constructor)]
pub struct BackendStat {
    free: u64,
}

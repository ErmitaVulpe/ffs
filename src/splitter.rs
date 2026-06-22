/// 4MiB
pub const DEFAULT_CHUNK_SIZE: u32 = 1 << 22;

pub struct Splitter {
    bytes_left: u64,
    goal_size: u32,
}

impl Splitter {
    pub fn new(len: u64) -> Self {
        if len == 0 {
            return Self {
                bytes_left: 0,
                goal_size: 1,
            };
        }

        let chunks = len.div_ceil(DEFAULT_CHUNK_SIZE as u64);
        let goal_size = len.div_ceil(chunks) as u32;

        Self {
            bytes_left: len,
            goal_size,
        }
    }
}

impl Iterator for Splitter {
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num = self.bytes_left.div_ceil(self.goal_size as u64) as usize;
        (num, Some(num))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new() {
        let split = Splitter::new(42);
        assert_eq!(split.bytes_left, 42);
        assert_eq!(split.goal_size, 42);

        let split = Splitter::new(DEFAULT_CHUNK_SIZE as u64);
        assert_eq!(split.bytes_left, DEFAULT_CHUNK_SIZE as u64);
        assert_eq!(split.goal_size, DEFAULT_CHUNK_SIZE);

        let split = Splitter::new(DEFAULT_CHUNK_SIZE as u64 + 1);
        assert_eq!(split.bytes_left, DEFAULT_CHUNK_SIZE as u64 + 1);
        assert_eq!(split.goal_size, DEFAULT_CHUNK_SIZE / 2 + 1);
    }

    #[test]
    fn iterator() {
        fn validate(mut splitter: Splitter) {
            let max_size = splitter.goal_size;
            assert!(splitter.all(|v| v <= max_size))
        }

        validate(Splitter::new(0));
        validate(Splitter::new(42));
        validate(Splitter::new(DEFAULT_CHUNK_SIZE as u64));
        validate(Splitter::new(DEFAULT_CHUNK_SIZE as u64 + 1));
        validate(Splitter::new(3 * DEFAULT_CHUNK_SIZE as u64 + 1));
    }

    #[test]
    fn size_hint() {
        fn validate(splitter: Splitter) {
            let size = splitter.size_hint();
            assert_eq!(size.0, size.1.unwrap());
            assert_eq!(splitter.collect::<Vec<_>>().len(), size.0);
        }

        validate(Splitter::new(0));
        validate(Splitter::new(42));
        validate(Splitter::new(DEFAULT_CHUNK_SIZE as u64));
        validate(Splitter::new(DEFAULT_CHUNK_SIZE as u64 + 1));
        validate(Splitter::new(3 * DEFAULT_CHUNK_SIZE as u64 + 1));
    }
}

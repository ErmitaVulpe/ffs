use std::str::FromStr;

use derive_more::{Display, Error};
use rkyv::{Archive, Deserialize, Serialize};

pub use crate::{
    App,
    backend::{BackendExt, BackendKindSpecifier},
};

/// Id of the backend instance
/// 0 is reserved for the bootstrap
pub type BackendId = u32;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct InodePath {
    segments: Vec<String>,
}

impl InodePath {
    pub fn pop(&mut self) -> Option<String> {
        self.segments.pop()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &str> {
        self.segments.iter().map(String::as_str)
    }
}

impl FromStr for InodePath {
    type Err = InodePathParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        let unprefixed = trimmed.strip_prefix('/').unwrap_or(trimmed);
        let unsuffixed = unprefixed.strip_suffix('/').unwrap_or(unprefixed);
        if unsuffixed.is_empty() {
            return Ok(Self::default());
        }
        let split = unsuffixed.split('/');

        let segments = split
            .map(|seg| {
                if seg.is_empty() {
                    Err(InodePathParseError::SegmentEmpty)
                } else if seg.len() > 255 {
                    Err(InodePathParseError::SegmentTooLong)
                } else {
                    Ok(seg.to_string())
                }
            })
            .collect::<Result<Vec<_>, InodePathParseError>>()?;

        Ok(Self { segments })
    }
}

#[derive(Debug, Display, Error)]
pub enum InodePathParseError {
    #[display("Path segment length exceeds 255 bytes")]
    SegmentTooLong,
    #[display("Path segment was empty")]
    SegmentEmpty,
}

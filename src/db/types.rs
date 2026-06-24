use std::str::FromStr;

use bytemuck::{Pod, Zeroable, bytes_of, from_bytes};
use compactly::Encode;
use derive_more::{Display, Error};
use redb::{MultimapTableDefinition, TableDefinition, TypeName, Value};

/// Id of the backend instance
pub type BackendId = u32;
pub type InodeId = u64;
/// Id of a data chunk
pub type ChunkId = u64;

/// Metadata of a specific backend. Contains encoded `BackendMeta`
pub const BACKENDS: TableDefinition<BackendId, &[u8]> = TableDefinition::new("BACKENDS");
/// Chunks making up the inodes
pub const CHUNKS: TableDefinition<ChunkId, ChunkData> = TableDefinition::new("CHUNKS");
pub const CHUNKS_TO_DROP: MultimapTableDefinition<(), ChunkId> =
    MultimapTableDefinition::new("CHUNKS_TO_DROP");
/// Metadata of an inode. Contains encoded `InodeMeta`
pub const INODES: TableDefinition<InodeId, &[u8]> = TableDefinition::new("INODES");
pub const CHUNKS_OF_INODES: MultimapTableDefinition<InodeId, ChunkId> =
    MultimapTableDefinition::new("CHUNKS_OF_INODES");

pub const INODE_RELATION_CHILDREN: MultimapTableDefinition<InodeId, InodeId> =
    MultimapTableDefinition::new("INODE_RELATION_CHILDREN");
pub const INODE_RELATION_PARENT: TableDefinition<InodeId, InodeId> =
    TableDefinition::new("INODE_RELATION_PARENT");

pub const METADATA: TableDefinition<u8, &[u8]> = TableDefinition::new("METADATA");

#[derive(Clone, Debug, Encode, PartialEq, Eq, PartialOrd, Ord)]
pub struct BackendMeta {
    pub free: u64,
    pub total: u64,
    pub chunks_contained: u32,
    pub kind: BackendKind,
}

#[derive(Clone, Debug, Display, Encode, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BackendKind {
    /// Dummy backend which stores chunks as files at the given path
    #[cfg(debug_assertions)]
    #[display("Dummy")]
    Dummy(String) = 0,
}

#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BackendKindSpecifier {
    /// Dummy backend which stores chunks as files at the given path
    #[cfg(debug_assertions)]
    #[display("Dummy")]
    Dummy = 0,
}

impl FromStr for BackendKindSpecifier {
    type Err = BackendParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let a = s.to_lowercase();
        Ok(match a.as_str() {
            "dummy" => Self::Dummy,
            _ => return Err(BackendParseError),
        })
    }
}

#[derive(Debug, Display, Error)]
#[display("Unsupported backend kind")]
pub struct BackendParseError;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Zeroable, Pod)]
pub struct ChunkData {
    pub offset: u64,
    pub length: u32,
    pub backend_id: BackendId,
    pub chunk_id: ChunkId,
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Encode)]
pub struct InodeFlags(u8);

bitflags::bitflags! {
    impl InodeFlags: u8 {
        const IS_FILE = 1; // 0 for a directory, 1 for a file
    }
}
unsafe impl Zeroable for InodeFlags {}
unsafe impl Pod for InodeFlags {}

#[repr(C)]
#[derive(Clone, Debug, Encode)]
pub struct InodeMeta {
    pub name: String,
    pub inode_flags: InodeFlags,
    pub size: u64,
}

impl InodeMeta {
    pub fn new_directory(name: String) -> Self {
        InodeMeta {
            size: 0,
            inode_flags: InodeFlags::empty(),
            name,
        }
    }

    pub fn new_file(name: String, size: u64) -> Self {
        InodeMeta {
            size,
            inode_flags: InodeFlags::IS_FILE,
            name,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        compactly::encode(self)
    }

    pub fn is_dir(&self) -> bool {
        !self.inode_flags.contains(InodeFlags::IS_FILE)
    }
}

macro_rules! impl_redb_value {
    ($t:ty) => {
        impl Value for $t {
            type SelfType<'a> = Self;
            type AsBytes<'a> = &'a [u8];

            fn fixed_width() -> Option<usize> {
                Some(size_of::<Self>())
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                *from_bytes(data)
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                bytes_of(value)
            }

            fn type_name() -> TypeName {
                TypeName::new(stringify!($t))
            }
        }
    };
}

impl_redb_value!(ChunkData);

/// Keys for the metadata table
#[allow(clippy::enum_variant_names)]
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Zeroable)]
pub enum Metadata {
    /// Stored as `BackendId`
    NextBackend = 0,
    /// Stored as `InodeId`
    NextInode = 1,
    /// Stored as `ChunkId`
    NextChunk = 2,
}

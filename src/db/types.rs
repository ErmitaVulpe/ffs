use std::{ops::Range, str::FromStr};

use compactly::Encode;
use derive_more::{Display, Error};
use redb::{MultimapTableDefinition, TableDefinition, TypeName, Value};

/// Id of the backend instance
pub type BackendId = u32;
pub type InodeId = u64;
/// Id of a data chunk
pub type ChunkId = u64;

/// Metadata of a specific backend
pub const BACKENDS: TableDefinition<BackendId, BackendMeta> = TableDefinition::new("BACKENDS");
/// Chunks making up the inodes
pub const CHUNKS: TableDefinition<ChunkId, ChunkData> = TableDefinition::new("CHUNKS");
/// Chunks marked to be dropped
pub const CHUNKS_TO_DROP: TableDefinition<ChunkId, ()> = TableDefinition::new("CHUNKS_TO_DROP");
/// Chunks for pending uploads
pub const TEMP_CHUNKS: TableDefinition<ChunkId, ()> = TableDefinition::new("TEMP_CHUNKS");
/// Metadata of an inode
pub const INODES: TableDefinition<InodeId, InodeMeta> = TableDefinition::new("INODES");
pub const CHUNKS_OF_INODES: MultimapTableDefinition<InodeId, ChunkId> =
    MultimapTableDefinition::new("CHUNKS_OF_INODES");

pub const INODE_RELATION_CHILDREN: MultimapTableDefinition<InodeId, InodeId> =
    MultimapTableDefinition::new("INODE_RELATION_CHILDREN");
pub const INODE_RELATION_PARENT: TableDefinition<InodeId, InodeId> =
    TableDefinition::new("INODE_RELATION_PARENT");

pub const METADATA: TableDefinition<u8, &[u8]> = TableDefinition::new("METADATA");

macro_rules! impl_redb_flex_value {
    ($t:ty) => {
        impl Value for $t {
            type SelfType<'a> = Self;
            type AsBytes<'a> = Vec<u8>;

            fn fixed_width() -> Option<usize> {
                None
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                compactly::decode(data).unwrap()
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                compactly::encode(value)
            }

            fn type_name() -> TypeName {
                TypeName::new(stringify!($t))
            }
        }
    };
}

#[derive(Clone, Debug, Encode, PartialEq, Eq, PartialOrd, Ord)]
pub struct BackendMeta {
    pub free: u64,
    pub total: u64,
    pub chunks_contained: u32,
    pub kind: BackendKind,
}

impl_redb_flex_value!(BackendMeta);

#[derive(Clone, Debug, Display, Encode, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BackendKind {
    /// Dummy backend which stores chunks as files at the given path
    #[cfg(debug_assertions)]
    #[display("Dummy")]
    Dummy(String) = 0,
    #[display("GoogleDrive")]
    GoogleDrive = 1,
}

#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BackendKindSpecifier {
    /// Dummy backend which stores chunks as files at the given path
    #[cfg(debug_assertions)]
    #[display("Dummy")]
    Dummy = 0,
    #[display("GoogleDrive")]
    GoogleDrive = 1,
}

impl FromStr for BackendKindSpecifier {
    type Err = BackendParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let a = s.to_lowercase();
        Ok(match a.as_str() {
            #[cfg(debug_assertions)]
            "dummy" => Self::Dummy,
            "google" | "googledrive" => Self::GoogleDrive,
            _ => return Err(BackendParseError),
        })
    }
}

#[derive(Debug, Display, Error)]
#[display("Unsupported backend kind")]
pub struct BackendParseError;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkData {
    pub inode_id: InodeId,
    pub offset: u64,
    pub length: u32,
    pub backend_id: BackendId,
}

impl ChunkData {
    pub fn as_range(&self) -> Range<usize> {
        let offset = self.offset as usize;
        offset..offset + self.length as usize
    }
}

impl Value for ChunkData {
    type SelfType<'a> = Self;
    type AsBytes<'a> = [u8; size_of::<Self>()];

    fn fixed_width() -> Option<usize> {
        Some(size_of::<Self>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        debug_assert_eq!(data.len(), size_of::<Self>());
        Self {
            inode_id: InodeId::from_le_bytes(data[0..8].try_into().unwrap()),
            offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            length: u32::from_le_bytes(data[16..20].try_into().unwrap()),
            backend_id: BackendId::from_le_bytes(data[20..24].try_into().unwrap()),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut bytes = [0u8; 24];
        bytes[0..8].copy_from_slice(&value.inode_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&value.offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&value.length.to_le_bytes());
        bytes[20..24].copy_from_slice(&value.backend_id.to_le_bytes());
        bytes
    }

    fn type_name() -> TypeName {
        TypeName::new("ffs::ChunkData")
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Encode)]
pub struct InodeFlags(u8);

bitflags::bitflags! {
    impl InodeFlags: u8 {
        const IS_FILE = 1; // 0 for a directory, 1 for a file
    }
}

#[derive(Clone, Debug, Encode)]
pub struct InodeMeta {
    pub name: String,
    pub size: u64,
    pub inode_flags: InodeFlags,
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

    pub fn is_dir(&self) -> bool {
        !self.inode_flags.contains(InodeFlags::IS_FILE)
    }
}

impl_redb_flex_value!(InodeMeta);

/// Keys for the metadata table
#[allow(clippy::enum_variant_names)]
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Metadata {
    /// Stored as `BackendId`
    NextBackend = 0,
    /// Stored as `InodeId`
    NextInode = 1,
    /// Stored as `ChunkId`
    NextChunk = 2,
}

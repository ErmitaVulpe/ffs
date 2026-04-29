use bytemuck::{Pod, Zeroable, bytes_of, from_bytes};
use redb::{Key, MultimapTableDefinition, TableDefinition, TypeName, Value};

/// Id of the backend instance
pub type BackendId = u32;
pub type InodeId = u64;
/// Id of a data chunk
pub type ChunkId = u64;

/// Metadata of a specific backend
pub const BACKENDS: TableDefinition<BackendId, BackendData> = TableDefinition::new("BACKENDS");
/// Metadata of a specific backend
pub const BACKENDS_INIT_DATA: TableDefinition<BackendId, &[u8]> =
    TableDefinition::new("BACKENDs_INIT_DATA");
/// Chunks making up the inode
pub const CHUNKS: MultimapTableDefinition<InodeId, ChunkData> =
    MultimapTableDefinition::new("CHUNKS");
pub const INODES: TableDefinition<InodeId, InodeMeta> = TableDefinition::new("INODES");

pub const INODE_RELATION_CHILDREN: MultimapTableDefinition<InodeId, InodeId> =
    MultimapTableDefinition::new("INODE_RELATION_CHILDREN");
pub const INODE_RELATION_PARENT: TableDefinition<InodeId, InodeId> =
    TableDefinition::new("INODE_RELATION_PARENT");

pub const METADATA: TableDefinition<u8, &[u8]> = TableDefinition::new("METADATA");

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Zeroable, Pod)]
pub struct BackendData {
    pub free: u64,
    pub total: u64,
    pub chunks_contained: u32,
    pub _padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Zeroable, Pod)]
pub struct ChunkData {
    pub offset: u64,
    pub length: u32,
    pub backend_id: BackendId,
    pub chunk_id: ChunkId,
}

impl Key for ChunkData {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1[0..8].cmp(&data2[0..8])
    }
}

bitflags::bitflags! {
    #[repr(transparent)]
    #[derive(Clone, Copy, Debug)]
    pub struct InodeFlags: u16 {
        const TYPE = 1; // 0 for a directory, 1 for a file
    }
}
unsafe impl Zeroable for InodeFlags {}
unsafe impl Pod for InodeFlags {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct InodeMeta {
    pub size: u64,
    pub _padding_u32: u32,
    pub _padding_u16: u16,
    pub inode_type: InodeFlags,
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

impl_redb_value!(BackendData);
impl_redb_value!(ChunkData);
impl_redb_value!(InodeMeta);

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Zeroable)]
pub enum Metadata {
    Backend = 0,
    Inode = 1,
    Chunk = 2,
}

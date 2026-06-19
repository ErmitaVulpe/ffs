use std::{path::Path, str::FromStr};

use derive_more::{Display, Error, From};
use redb::{ReadableDatabase, ReadableTable};

use crate::db::types::{
    INODE_RELATION_CHILDREN, INODE_RELATION_PARENT, INODES, InodeId, METADATA, Metadata,
};

mod types;

pub use types::InodeMeta;

pub struct Db {
    redb: redb::Database,
}

impl Db {
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let db_file_exists = std::fs::metadata(db_path.as_ref()).is_ok();

        let db = if db_file_exists {
            redb::Database::open(db_path)?
        } else {
            let mut db = redb::Database::create(db_path)?;
            // batabse tables need to get initialized
            let txn = db.begin_write()?;

            // init tables
            {
                txn.open_table(types::BACKENDS)?;
                txn.open_table(types::CHUNKS)?;

                let mut inodes = txn.open_table(types::INODES)?;
                // init root inode
                inodes.insert(
                    0,
                    &InodeMeta::new_directory(String::new()).encode().as_slice(),
                )?;

                txn.open_multimap_table(types::CHUNKS_OF_INODES)?;
                txn.open_multimap_table(types::INODE_RELATION_CHILDREN)?;
                txn.open_table(types::INODE_RELATION_PARENT)?;

                let mut metadata = txn.open_table(types::METADATA)?;
                metadata.insert(
                    types::Metadata::NextBackend as u8,
                    (0 as types::BackendId).to_le_bytes().as_slice(),
                )?;
                metadata.insert(
                    types::Metadata::NextInode as u8,
                    // this inits at 1 since root inode is reserved
                    (1 as types::InodeId).to_le_bytes().as_slice(),
                )?;
                metadata.insert(
                    types::Metadata::NextChunk as u8,
                    (0 as types::ChunkId).to_le_bytes().as_slice(),
                )?;
            }

            txn.commit()?;
            db.compact()?;
            db
        };

        Ok(Self { redb: db })
    }

    /// Returns an iterator over children of an inode
    pub fn iter_children(
        &self,
        id: InodeId,
    ) -> Result<impl Iterator<Item = Result<(u64, InodeMeta), LookupError>>, LookupError> {
        let txn = self.redb.begin_read()?;
        let relations = txn.open_multimap_table(INODE_RELATION_CHILDREN)?;
        let inodes = txn.open_table(INODES)?;

        Ok(relations
            .get(id)
            .map_err(|_| LookupError::Corrupted)?
            .map(move |id| {
                id.map_err(LookupError::from).and_then(|id| {
                    let id = id.value();
                    inodes
                        .get(id)
                        .map_err(LookupError::from)
                        .and_then(|o| match o {
                            Some(v) => Ok(compactly::decode::<InodeMeta>(v.value())
                                .ok_or(LookupError::Corrupted)),
                            None => Err(LookupError::Corrupted),
                        })
                        .flatten()
                        .map(|v| (id, v))
                })
            }))
    }

    /// Maps the `InodePath` to a `InodeId`, returns `Ok(None)` if no such file exists
    pub fn inode_lookup(&self, path: &InodePath) -> Result<Option<InodeId>, LookupError> {
        let mut current_inode = 0 as InodeId;

        'seg: for seg in &path.segments {
            for child in self.iter_children(current_inode)? {
                let (id, meta) = child?;
                if &meta.name == seg {
                    current_inode = id;
                    continue 'seg;
                }
            }

            return Ok(None);
        }

        Ok(Some(current_inode))
    }

    pub fn create_inode(
        &self,
        parent: InodeId,
        meta: InodeMeta,
    ) -> Result<InodeId, CreateInodeError> {
        let name_conflict = self
            .iter_children(parent)?
            .any(|r| r.map(|(_, m)| m.name == meta.name).unwrap_or(false));
        if name_conflict {
            return Err(CreateInodeError::NameConflict);
        }

        let txn = self.redb.begin_write()?;
        let new_inode_id = {
            let mut inodes = txn.open_table(INODES)?;

            let parent_meta = if let Some(meta) = inodes.get(parent)? {
                compactly::decode::<InodeMeta>(meta.value()).ok_or(CreateInodeError::Corrupted)?
            } else {
                return Err(CreateInodeError::AttemptedOrphan);
            };

            if !parent_meta.is_dir() {
                return Err(CreateInodeError::ParentNotDir);
            }

            let mut meta_table = txn.open_table(METADATA)?;
            let mut next_id_guard = meta_table
                .get_mut(Metadata::NextInode as u8)?
                .ok_or(CreateInodeError::Corrupted)?;
            let new_inode_id = InodeId::from_le_bytes(
                *next_id_guard
                    .value()
                    .as_array()
                    .ok_or(CreateInodeError::Corrupted)?,
            );

            let res = inodes.insert(&new_inode_id, meta.encode().as_slice())?;
            debug_assert!(res.is_none());
            next_id_guard.insert(
                new_inode_id
                    .checked_add(1)
                    .ok_or(CreateInodeError::OutOfIds)?
                    .to_le_bytes()
                    .as_slice(),
            )?;

            let mut children_rel = txn.open_multimap_table(INODE_RELATION_CHILDREN)?;
            let res = children_rel.insert(parent, new_inode_id)?;
            debug_assert!(!res);
            let mut parent_rel = txn.open_table(INODE_RELATION_PARENT)?;
            let res = parent_rel.insert(new_inode_id, parent)?;
            debug_assert!(res.is_none());

            new_inode_id
        };
        txn.commit()?;

        Ok(new_inode_id)
    }
}

macro_rules! impl_from_redb {
    ($typ:ty => $nam:ident, $($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for $typ {
                fn from(value: $ty) -> Self {
                    Self::$nam(value.into())
                }
            }
        )*
    };
}

#[derive(Debug, Display, Error, From)]
#[display("Failed to find the file")]
pub enum LookupError {
    Db(redb::Error),
    #[display("Failed to decode inode metadata. Db is most likely corrupted")]
    Corrupted,
}

impl_from_redb!(
    LookupError => Db,
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
);

#[derive(Debug, Display, Error, From)]
#[display("Failed to find the file")]
pub enum CreateInodeError {
    Db(redb::Error),
    #[display("Specified parent doesnt exist")]
    AttemptedOrphan,
    #[display("Db is corrupted")]
    Corrupted,
    #[display("This name is already in use")]
    NameConflict,
    #[display("Parent is not a directory")]
    ParentNotDir,
    #[display("Ran out if inode ids")]
    OutOfIds,
}

impl_from_redb!(
    CreateInodeError => Db,
    redb::CommitError,
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
);

impl From<LookupError> for CreateInodeError {
    fn from(value: LookupError) -> Self {
        match value {
            LookupError::Db(error) => error.into(),
            LookupError::Corrupted => Self::Corrupted,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct InodePath {
    segments: Vec<String>,
}

impl InodePath {
    pub fn pop(&mut self) -> Option<String> {
        self.segments.pop()
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

use std::{
    collections::{BTreeMap, HashMap, btree_map::Entry},
    ops::{RangeBounds, RangeInclusive},
    path::Path,
    str::FromStr,
    sync::RwLock,
};

use derive_more::{Display, Error, From};
use redb::{ReadableDatabase, ReadableMultimapTable, ReadableTable};

use crate::{
    chunk_alloc::BackendStat,
    db::types::{
        BACKENDS, CHUNKS, CHUNKS_OF_INODES, CHUNKS_TO_DROP, INODE_RELATION_CHILDREN,
        INODE_RELATION_PARENT, INODES, InodeId, METADATA, Metadata, TEMP_CHUNKS,
    },
};

mod types;

pub use types::{
    BackendId, BackendKind, BackendKindSpecifier, BackendMeta, BackendParseError, ChunkData,
    ChunkId, InodeFlags, InodeMeta,
};

pub type Result<T, E = DbError> = std::result::Result<T, E>;

pub struct Db {
    redb: RwLock<redb::Database>,
}

impl Db {
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self> {
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
                txn.open_table(types::CHUNKS_TO_DROP)?;
                txn.open_table(types::TEMP_CHUNKS)?;

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
                    // this inits at 0 is reserved for marker chunks in backends
                    (1 as types::ChunkId).to_le_bytes().as_slice(),
                )?;
            }

            txn.commit()?;
            db.compact()?;
            db
        };

        Ok(Self {
            redb: RwLock::new(db),
        })
    }

    fn get_redb(&self) -> std::sync::RwLockReadGuard<'_, redb::Database> {
        self.redb.read().unwrap()
    }

    pub fn app_init_data(&self) -> Result<AppInitData> {
        let backends_stat = self
            .get_redb()
            .begin_read()?
            .open_table(BACKENDS)?
            .iter()?
            .map(|r| {
                r.map_err(DbError::from).and_then(|(k, v)| {
                    let meta = compactly::decode::<BackendMeta>(v.value())
                        .ok_or(DbError::Corrupted("Failed to read backend metadata"))?;
                    let v = BackendStat::new(meta.free);
                    Ok((k.value(), v))
                })
            })
            .collect::<Result<HashMap<BackendId, BackendStat>>>()?;
        Ok(AppInitData { backends_stat })
    }

    /// Returns an iterator over children of an inode
    pub fn iter_children(
        &self,
        id: InodeId,
    ) -> Result<impl Iterator<Item = Result<(u64, InodeMeta)>>> {
        let txn = self.get_redb().begin_read()?;
        let relations = txn.open_multimap_table(INODE_RELATION_CHILDREN)?;
        let inodes = txn.open_table(INODES)?;

        Ok(relations
            .get(id)
            .map_err(|_| DbError::Corrupted(""))?
            .map(move |id| {
                id.map_err(DbError::from).and_then(|id| {
                    let id = id.value();
                    inodes
                        .get(id)
                        .map_err(DbError::from)
                        .and_then(|o| match o {
                            Some(v) => Ok(compactly::decode::<InodeMeta>(v.value())
                                .ok_or(DbError::CorruptedData)),
                            None => Err(DbError::CorruptedStructure),
                        })
                        .flatten()
                        .map(|v| (id, v))
                })
            }))
    }

    /// Returns `true` if compaction was performed, and `false` if no futher compaction was possible
    pub fn compact(&self) -> Result<bool> {
        self.redb.write().unwrap().compact().map_err(DbError::from)
    }

    /// Maps the `InodePath` to a `InodeId`, returns `Ok(None)` if no such file exists
    pub fn inode_lookup(&self, path: &InodePath) -> Result<Option<InodeId>> {
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

    pub fn inode_meta(&self, inode: InodeId) -> Result<Option<InodeMeta>> {
        self.get_redb()
            .begin_read()?
            .open_table(INODES)?
            .get(&inode)
            .map_err(DbError::from)
            .map(|o| o.and_then(|a| compactly::decode::<InodeMeta>(a.value())))
    }

    pub fn create_inode(&self, parent: InodeId, id: InodeId, meta: InodeMeta) -> Result<()> {
        let name_conflict = self
            .iter_children(parent)?
            .any(|r| r.map(|(_, m)| m.name == meta.name).unwrap_or(false));
        if name_conflict {
            return Err(DbError::NameConflict);
        }

        let txn = self.get_redb().begin_write()?;

        {
            let mut inodes = txn.open_table(INODES)?;

            let parent_meta = if let Some(meta) = inodes.get(parent)? {
                compactly::decode::<InodeMeta>(meta.value()).ok_or(DbError::CorruptedData)?
            } else {
                return Err(DbError::AttemptedOrphan);
            };

            if !parent_meta.is_dir() {
                return Err(DbError::ParentNotDir);
            }

            let res = inodes.insert(&id, meta.encode().as_slice())?;
            debug_assert!(res.is_none());

            let mut children_rel = txn.open_multimap_table(INODE_RELATION_CHILDREN)?;
            let res = children_rel.insert(parent, id)?;
            debug_assert!(!res);
            let mut parent_rel = txn.open_table(INODE_RELATION_PARENT)?;
            let res = parent_rel.insert(id, parent)?;
            debug_assert!(res.is_none());
        }

        txn.commit()?;
        Ok(())
    }

    pub fn remove_inode(&self, inode: InodeId) -> Result<()> {
        let txn = self.get_redb().begin_write()?;

        {
            let mut inodes = txn.open_table(INODES)?;
            let meta = compactly::decode::<InodeMeta>(
                inodes
                    .get(inode)?
                    .ok_or(DbError::NotFound(OutOfIdsKind::Inode))?
                    .value(),
            )
            .ok_or(DbError::NotFound(OutOfIdsKind::Inode))?;

            match meta.inode_flags.contains(InodeFlags::IS_FILE) {
                true => {
                    if meta.size != 0 {
                        let mut chunks_of_inodes = txn.open_multimap_table(CHUNKS_OF_INODES)?;
                        let mut chunks_to_drop = txn.open_table(CHUNKS_TO_DROP)?;
                        let res = chunks_of_inodes
                            .remove_all(inode)?
                            .map(|r| {
                                r.and_then(|cid| {
                                    chunks_to_drop
                                        .insert(cid.value(), ())
                                        // Everything from this line to the empty line is just to
                                        // check correctness
                                        .map(|o| o.map(|_| ()))
                                })
                            })
                            .try_fold(false, |acc, r| {
                                Ok::<bool, redb::StorageError>(acc || r?.is_some())
                            })?;
                        debug_assert!(!res);

                        todo!("This needs to spawn the cleanup");
                    }
                }
                false => {
                    let children = txn.open_multimap_table(INODE_RELATION_CHILDREN)?;
                    if !children.get(inode)?.is_empty() {
                        return Err(DbError::HasChildren);
                    }
                }
            }

            inodes.remove(inode)?;
            let parent_id = txn
                .open_table(INODE_RELATION_PARENT)?
                .remove(inode)?
                .ok_or(DbError::CorruptedStructure)?
                .value();
            let mut relation_children = txn.open_multimap_table(INODE_RELATION_CHILDREN)?;
            let res = relation_children.remove(parent_id, inode)?;
            debug_assert!(res);
        }

        txn.commit()?;
        Ok(())
    }

    pub fn new_backend_id(&self) -> Result<BackendId> {
        let txn = self.get_redb().begin_write()?;

        let new_id = {
            let mut meta_table = txn.open_table(METADATA)?;
            let mut next_id_guard = meta_table
                .get_mut(Metadata::NextBackend as u8)?
                .ok_or(DbError::CorruptedStructure)?;
            let new_id = BackendId::from_le_bytes(
                *next_id_guard
                    .value()
                    .as_array()
                    .ok_or(DbError::CorruptedData)?,
            );
            next_id_guard.insert(
                new_id
                    .checked_add(1)
                    .ok_or(DbError::OutOfIds(OutOfIdsKind::Backend))?
                    .to_le_bytes()
                    .as_slice(),
            )?;

            new_id
        };

        txn.commit()?;
        Ok(new_id)
    }

    pub fn add_backend(&self, id: BackendId, meta: BackendMeta) -> Result<()> {
        let txn = self.get_redb().begin_write()?;

        let res = {
            let mut backends_table = txn.open_table(BACKENDS)?;
            let res = backends_table.insert(id, compactly::encode(&meta).as_slice())?;
            if res.is_some() {
                Err(DbError::DuplicateId(OutOfIdsKind::Backend))
            } else {
                Ok(())
            }
        };

        if res.is_ok() {
            txn.commit()?;
        } else {
            txn.abort()?;
        }
        res
    }

    pub fn get_backend(&self, id: &BackendId) -> Result<Option<BackendMeta>> {
        let txn = self.get_redb().begin_read()?;
        let table = txn.open_table(BACKENDS)?;
        table.get(id).map_err(DbError::from).and_then(|o| {
            o.map(|v| compactly::decode::<BackendMeta>(v.value()).ok_or(DbError::CorruptedData))
                .transpose()
        })
    }

    pub fn list_backends(&self) -> Result<impl Iterator<Item = Result<(BackendId, BackendMeta)>>> {
        let txn = self.get_redb().begin_read()?;
        let table = txn.open_table(BACKENDS)?;
        // .range has to be used here because it returns `Range<'static, K, V>`, and .iter returns
        // `Range<'_, K, V>`
        let backends = table.range(0..=u32::MAX)?.map(|e| {
            e.map_err(DbError::from).and_then(|pair| {
                let k = pair.0.value();
                let v = compactly::decode::<BackendMeta>(pair.1.value())
                    .ok_or(DbError::CorruptedData)?;
                Ok((k, v))
            })
        });
        Ok(backends)
    }

    pub fn reserve_chunk_ids(&self, count: u64) -> Result<RangeInclusive<ChunkId>> {
        let txn = self.get_redb().begin_write()?;

        let range = {
            let mut table = txn.open_table(METADATA)?;
            let mut next_id_guard = table
                .get_mut(Metadata::NextChunk as u8)?
                .ok_or_else(|| redb::Error::Corrupted("Missing kv store".to_string()))?;
            let next_id = ChunkId::from_le_bytes(
                *next_id_guard
                    .value()
                    .as_array()
                    .ok_or_else(|| redb::Error::Corrupted("Corrupted kv store".to_string()))?,
            );

            let new_next_id = next_id
                .checked_add(count)
                .ok_or(DbError::OutOfIds(OutOfIdsKind::Chunk))?;
            next_id_guard.insert(new_next_id.to_le_bytes().as_slice())?;
            next_id..=new_next_id - 1
        };

        txn.commit()?;
        debug_assert_eq!(range.clone().count() as u64, count);
        Ok(range)
    }

    pub fn reserve_inode_id(&self) -> Result<InodeId> {
        let txn = self.get_redb().begin_write()?;

        let next_id = {
            let mut table = txn.open_table(METADATA)?;
            let mut next_id_guard = table
                .get_mut(Metadata::NextInode as u8)?
                .ok_or_else(|| redb::Error::Corrupted("Missing kv store".to_string()))?;
            let next_id = ChunkId::from_le_bytes(
                *next_id_guard
                    .value()
                    .as_array()
                    .ok_or_else(|| redb::Error::Corrupted("Corrupted kv store".to_string()))?,
            );

            let new_next_id = next_id
                .checked_add(1)
                .ok_or(DbError::OutOfIds(OutOfIdsKind::Inode))?;
            next_id_guard.insert(new_next_id.to_le_bytes().as_slice())?;
            next_id
        };

        txn.commit()?;
        Ok(next_id)
    }

    pub fn add_temp_chunks<'a>(
        &self,
        temp_chunks: impl Iterator<Item = &'a (ChunkId, ChunkData)>,
    ) -> Result<()> {
        let txn = self.get_redb().begin_write()?;

        {
            let mut chunks_table = txn.open_table(CHUNKS)?;
            let mut temp_chunks_table = txn.open_table(TEMP_CHUNKS)?;
            for (id, chunk) in temp_chunks {
                chunks_table.insert(id, chunk)?;
                temp_chunks_table.insert(id, ())?;
            }
        }

        txn.commit()?;
        Ok(())
    }

    /// Marks temp chunks as to delete. Returns true if any chunks were moved
    pub fn cancel_temp_chunks(&self, ids: impl RangeBounds<u64>) -> Result<bool> {
        let txn = self.get_redb().begin_write()?;

        let is_empty = {
            let mut temp_chunks = txn.open_table(TEMP_CHUNKS)?;
            let mut chunks_to_drop = txn.open_table(CHUNKS_TO_DROP)?;
            let result = temp_chunks
                .extract_from_if(ids, |_, _| true)?
                .map(|r| {
                    r.and_then(|(k, _)| chunks_to_drop.insert(k.value(), ()).map(|o| o.map(|_| ())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            debug_assert!(result.iter().all(Option::is_none));
            result.is_empty()
        };

        txn.commit()?;
        Ok(!is_empty)
    }

    /// Marks temp chunks as not temp
    pub fn commit_temp_chunks(&self, ids: impl RangeBounds<u64>) -> Result<()> {
        let txn = self.get_redb().begin_write()?;

        {
            let chunks_table = txn.open_table(CHUNKS)?;
            let mut temp_chunks_table = txn.open_table(TEMP_CHUNKS)?;
            let mut space_to_sub = BTreeMap::new();

            for res in temp_chunks_table.extract_from_if(ids, |_, _| true)? {
                let id = res?.0.value();
                let meta = chunks_table
                    .get(&id)?
                    .ok_or(DbError::CorruptedStructure)?
                    .value();
                *space_to_sub.entry(meta.backend_id).or_insert(0) += meta.length as u64;
            }

            let mut backends = txn.open_table(BACKENDS)?;

            for (id, ammount) in space_to_sub {
                let mut guard = backends.get_mut(&id)?.ok_or(DbError::CorruptedStructure)?;
                let mut meta = compactly::decode::<BackendMeta>(guard.value())
                    .ok_or(DbError::CorruptedData)?;
                meta.free = meta
                    .free
                    .checked_sub(ammount)
                    .ok_or(DbError::CorruptedStructure)?;
                guard.insert(compactly::encode(&meta).as_slice())?;
            }
        }

        txn.commit()?;
        Ok(())
    }
}

pub struct AppInitData {
    pub backends_stat: HashMap<BackendId, BackendStat>,
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
#[display("Operation on the database failed")]
pub enum DbError {
    Internal(redb::Error),
    #[display("Db is corrupted: {_0}")]
    #[from(ignore)]
    Corrupted(#[error(ignore)] &'static str),
    #[display("Corrupted data in the database")]
    CorruptedData,
    #[display("Corrupted database relations")]
    CorruptedStructure,
    #[display("Ran out if {_0} ids")]
    #[from(ignore)]
    OutOfIds(#[error(ignore)] OutOfIdsKind),
    #[display("{_0} with this id already exists")]
    #[from(ignore)]
    DuplicateId(#[error(ignore)] OutOfIdsKind),
    #[display("{_0} with the specified id doesnt exist")]
    #[from(ignore)]
    NotFound(#[error(ignore)] OutOfIdsKind),
    #[display("Specified parent doesnt exist")]
    AttemptedOrphan,
    #[display("This name is already in use")]
    NameConflict,
    #[display("Parent is not a directory")]
    ParentNotDir,
    #[display("Attempted to remove a non empty directory")]
    HasChildren,
}

impl_from_redb!(
    DbError => Internal,
    redb::CommitError,
    redb::CompactionError,
    redb::DatabaseError,
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
);

#[derive(Debug, Display)]
pub enum OutOfIdsKind {
    #[display("backend")]
    Backend,
    #[display("inode")]
    Inode,
    #[display("chunk")]
    Chunk,
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

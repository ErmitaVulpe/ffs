use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
pub use db::{BackendKindSpecifier, BackendParseError, InodeMeta, InodePath, InodePathParseError};
use derive_more::{Display, Error, From};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
};

use crate::{
    backend::{Backend, BackendError, InitError},
    chunk_alloc::ChunkAlloc,
    db::{BackendId, BackendMeta, ChunkData, DbError, InodeFlags},
    splitter::Splitter,
};

mod backend;
mod chunk_alloc;
mod db;
mod splitter;

pub struct App {
    backends: RwLock<HashMap<BackendId, Arc<dyn Backend>>>,
    chunk_alloc: Mutex<ChunkAlloc>,
    db: db::Db,
}

impl App {
    pub fn new(db_path: impl AsRef<std::path::Path>) -> anyhow::Result<Arc<Self>> {
        let db = db::Db::new(db_path)?;
        let read_tx = db.read()?;
        let init_data = read_tx.app_init_data()?;
        let round_robin = Mutex::new(ChunkAlloc::new(init_data.backends_stat)?);
        let backends = RwLock::new(HashMap::new());
        Ok(Arc::new(Self {
            backends,
            chunk_alloc: round_robin,
            db,
        }))
    }

    pub async fn add_backend(&self, kind: BackendKindSpecifier) -> anyhow::Result<()> {
        let init_data = backend::generate_backend(kind)
            .await
            .context("Failed to generate new backend data")?;

        let txn = self.db.write()?;
        let id = txn.new_backend_id()?;
        txn.commit()?;
        let instance = backend::init(id, init_data.clone()).await?;
        let stat = instance
            .stat()
            .await
            .context("Failed to get stats of the new backend. Aborting")?;

        // Upload a marker chunk, if this fails, this backend is a duplicate
        instance.upload(0, &[]).await?;

        {
            let mut lock = self.backends.write().await;
            let res = lock.insert(id, instance);
            debug_assert!(res.is_none());
        }

        let meta = BackendMeta {
            total: stat.total,
            free: stat.total - stat.used,
            kind: init_data,
            chunks_contained: 0,
        };

        let txn = self.db.write()?;
        txn.add_backend(id, meta)?;
        txn.commit()?;
        Ok(())
    }

    async fn get_backend(&self, id: BackendId) -> Result<Arc<dyn Backend>, GetBackendError> {
        if let Some(backend) = self.backends.read().await.get(&id) {
            return Ok(backend.clone());
        }

        let txn = self.db.read()?;
        let meta = txn
            .get_backend(&id)?
            .ok_or(GetBackendError::NoSuchBackend)?;

        let backend = backend::init(id, meta.kind).await?;
        self.backends.write().await.insert(id, backend.clone());
        Ok(backend)
    }

    pub fn compact_db(&self) -> Result<bool, DbError> {
        self.db.compact()
    }

    pub fn list_backends(&self) -> db::Result<Vec<(BackendId, BackendMeta)>> {
        let txn = self.db.read()?;
        txn.list_backends()?
            .collect::<db::Result<Vec<(BackendId, BackendMeta)>>>()
    }

    pub fn read_dir(&self, path: &InodePath) -> anyhow::Result<Vec<(u64, InodeMeta)>> {
        let txn = self.db.read()?;
        let inode = txn.inode_lookup(path)?.context("Directory not found")?;
        let res = txn
            .iter_children(inode)?
            .map(|r| r.map_err(anyhow::Error::from))
            .collect::<anyhow::Result<Vec<(u64, InodeMeta)>>>()?;
        Ok(res)
    }

    pub fn mkdir(&self, mut path: InodePath) -> anyhow::Result<()> {
        let name = path.pop().context("No directory name specified")?;
        let meta = InodeMeta::new_directory(name);

        let txn = self.db.write()?;
        let parent_inode = txn
            .inode_lookup(&path)?
            .context("Parent directory doesnt exist")?;
        let inode = txn.reserve_inode_id()?;
        txn.create_inode(parent_inode, inode, meta)?;
        txn.commit()?;
        Ok(())
    }

    pub fn rm(&self, path: &InodePath) -> anyhow::Result<()> {
        let txn = self.db.write()?;
        let inode = txn
            .inode_lookup(path)?
            .context("File or directory not found")?;
        txn.remove_inode(inode)?;
        txn.commit()?;
        Ok(())
    }

    pub async fn upload_buf(
        self: &Arc<Self>,
        mut destination: InodePath,
        buf: Arc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        // Check if destination path is valid
        let filename = destination
            .pop()
            .context("Destination path needs to include a filename")?;
        let txn = self.db.write()?;
        let parent_inode = txn
            .inode_lookup(&destination)?
            .context("Parent dir does not exist")?;
        let parent_meta = txn
            .inode_meta(parent_inode)?
            .ok_or_else(|| redb::Error::Corrupted("Dangling inode id".to_string()))?;

        if !parent_meta.is_dir() {
            return Err(anyhow::anyhow!("Specified parent is not a directory"));
        }

        let inode_id = txn.reserve_inode_id()?;

        // Prepare for upload
        let splitter = Splitter::new(buf.len() as u64);
        let number_of_chunks = splitter.len();
        let ids = txn.reserve_chunk_ids(number_of_chunks as u64)?;
        txn.commit()?;
        let mut alloc = self.chunk_alloc.lock().await;
        let upload_plan = ids
            .clone()
            .zip(splitter)
            .scan(0, |offset, (id, len)| {
                let Some(backend_id) = alloc.allocate(len) else {
                    return Some(Err(anyhow::anyhow!("Out of space")));
                };
                let chunk_data = ChunkData {
                    inode_id,
                    offset: *offset,
                    length: len,
                    backend_id,
                };
                *offset += len as u64;
                Some(Ok((id, chunk_data)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        drop(alloc);

        let txn = self.db.write()?;
        txn.add_temp_chunks(upload_plan.iter())?;
        txn.commit()?;

        // Start uploading chunks
        let mut handles = JoinSet::new();
        for (id, chunk) in upload_plan {
            let local_self = self.clone();
            let buf = buf.clone();
            handles.spawn(async move {
                local_self
                    .get_backend(chunk.backend_id)
                    .await?
                    .upload(id, &buf[chunk.as_range()])
                    .await?;
                anyhow::Ok(())
            });
        }

        // Wait for uploading to finish and handle errors
        loop {
            match handles.join_next().await {
                None => break Ok(()),
                Some(res) => {
                    let res = res.map_err(anyhow::Error::from).flatten();
                    if let Err(err) = res {
                        handles.abort_all();
                        let txn = self.db.write()?;
                        txn.cancel_temp_chunks(ids.clone())?;
                        txn.commit()?;
                        break Err(err);
                    }
                }
            }
        }?;

        // Finish up the upload
        let meta = InodeMeta {
            name: filename,
            inode_flags: InodeFlags::IS_FILE,
            size: buf.len() as u64,
        };

        let txn = self.db.write()?;
        txn.commit_temp_chunks(ids)?;
        txn.create_inode(parent_inode, inode_id, meta)?;
        txn.commit()?;
        Ok(())
    }
}

#[derive(Debug, Display, Error, From)]
pub enum GetBackendError {
    Db(DbError),
    #[display("Tried to get a non existing backend")]
    NoSuchBackend,
    #[display("Failed to init the backend")]
    Backend(BackendError<InitError>),
}

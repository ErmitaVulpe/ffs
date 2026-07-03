use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
pub use db::{BackendKindSpecifier, BackendParseError, InodeMeta, InodePath, InodePathParseError};
use derive_more::{Display, Error, From};
use tokio::sync::{Mutex, RwLock};

use crate::{
    backend::{Backend, BackendError, InitError},
    chunk_alloc::ChunkAlloc,
    db::{BackendId, BackendMeta, ChunkData, ListBackendsError, LookupError},
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
    pub fn new(db_path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let db = db::Db::new(db_path)?;
        let init_data = db.app_init_data()?;
        let round_robin = Mutex::new(ChunkAlloc::new(init_data.backends_stat)?);
        let backends = RwLock::new(HashMap::new());
        Ok(Self {
            backends,
            chunk_alloc: round_robin,
            db,
        })
    }

    pub async fn add_backend(&self, kind: BackendKindSpecifier) -> anyhow::Result<()> {
        let init_data = backend::generate_backend(kind)
            .await
            .context("Failed to generate new backend data")?;

        let id = self.db.new_backend_id()?;
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

        self.db.add_backend(id, meta)?;
        Ok(())
    }

    async fn get_backend(&self, id: BackendId) -> Result<Arc<dyn Backend>, GetBackendError> {
        if let Some(backend) = self.backends.read().await.get(&id) {
            return Ok(backend.clone());
        }

        let meta = self
            .db
            .get_backend(&id)?
            .ok_or(GetBackendError::NoSuchBackend)?;

        let backend = backend::init(id, meta.kind).await?;
        self.backends.write().await.insert(id, backend.clone());
        Ok(backend)
    }

    pub fn compact_db(&mut self) -> Result<bool, redb::CompactionError> {
        self.db.compact()
    }

    pub fn list_backends(
        &self,
    ) -> Result<
        impl Iterator<Item = Result<(BackendId, BackendMeta), ListBackendsError>>,
        ListBackendsError,
    > {
        self.db.list_backends()
    }

    pub fn read_dir(
        &self,
        path: &InodePath,
    ) -> anyhow::Result<impl Iterator<Item = Result<(u64, InodeMeta), LookupError>>> {
        let inode = self.db.inode_lookup(path)?.context("Directory not found")?;
        let res = self.db.iter_children(inode)?;
        Ok(res)
    }

    pub fn mkdir(&self, mut path: InodePath) -> anyhow::Result<()> {
        let name = path.pop().context("No directory name specified")?;
        let inode = InodeMeta::new_directory(name);
        let parent_inode = self
            .db
            .inode_lookup(&path)?
            .context("Parent directory doesnt exist")?;
        self.db.create_inode(parent_inode, inode)?;
        Ok(())
    }

    pub fn rm(&self, path: &InodePath) -> anyhow::Result<()> {
        let inode = self
            .db
            .inode_lookup(path)?
            .context("File or directory not found")?;
        self.db.remove_inode(inode)?;
        Ok(())
    }

    pub async fn upload_buf(&self, destination: InodePath, buf: &[u8]) -> anyhow::Result<()> {
        let splitter = Splitter::new(buf.len() as u64);
        let number_of_chunks = splitter.len();
        let ids = self.db.reserve_chunk_ids(number_of_chunks as u64)?;
        let mut alloc = self.chunk_alloc.lock().await;
        let upload_plan = ids
            .zip(splitter)
            .scan(0, |offset, (id, len)| {
                let Some(backend_id) = alloc.allocate(len) else {
                    return Some(Err(anyhow::anyhow!("Out of space")));
                };
                let chunk_data = ChunkData {
                    offset: *offset,
                    length: len,
                    backend_id,
                    chunk_id: id,
                };
                *offset += len as u64;
                Some(Ok(chunk_data))
            })
            .collect::<Result<Vec<_>, _>>()?;

        println!("{upload_plan:#?}");

        Ok(())
    }
}

#[derive(Debug, Display, Error, From)]
pub enum GetBackendError {
    Db(redb::Error),
    #[display("Tried to get a non existing backend")]
    NoSuchBackend,
    #[display("Failed to init the backend")]
    Backend(BackendError<InitError>),
}

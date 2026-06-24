mod backend;
mod db;
mod splitter;

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
pub use db::{BackendKindSpecifier, BackendParseError, InodeMeta, InodePath, InodePathParseError};
use tokio::sync::RwLock;

use crate::{
    backend::Backend,
    db::{BackendId, BackendMeta, ListBackendsError, LookupError},
};

pub struct App {
    backends: RwLock<HashMap<BackendId, Arc<dyn Backend>>>,
    db: db::Db,
}

impl App {
    pub fn new(db_path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let db = db::Db::new(db_path)?;
        let backends = RwLock::new(HashMap::new());
        Ok(Self { backends, db })
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

    pub async fn upload_file() {}
}

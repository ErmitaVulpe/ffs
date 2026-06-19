mod backend;
mod db;

use anyhow::Context;
pub use db::{InodeMeta, InodePath, InodePathParseError};

use crate::db::LookupError;

pub struct App {
    db: db::Db,
}

impl App {
    pub fn new(db_path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let db = db::Db::new(db_path)?;
        Ok(Self { db })
    }

    pub fn compact_db(&mut self) -> Result<bool, redb::CompactionError> {
        self.db.compact()
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

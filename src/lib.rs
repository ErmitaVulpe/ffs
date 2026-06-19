mod backend;
mod db;

pub use db::{InodeMeta, InodePath, InodePathParseError};

pub struct App {
    pub db: db::Db,
}

impl App {
    pub fn new(db_path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let db = db::Db::new(db_path)?;
        Ok(Self { db })
    }
}

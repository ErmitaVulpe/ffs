use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
};

use crate::splitter::DEFAULT_CHUNK_SIZE;

use super::*;

pub struct DummyImpl;

#[async_trait]
impl BackendMod for DummyImpl {
    type BackendImpl = DummyBackend;
    type InitCtx = String;

    async fn init(
        id: BackendId,
        ctx: Self::InitCtx,
    ) -> Result<Arc<dyn Backend>, BackendError<InitError>> {
        let result = fs::metadata(&ctx).await;
        match result {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(BackendError::new(
                        id,
                        InitError(anyhow::anyhow!("Specified path is not a directory")),
                    ));
                }
            }
            Err(e) => {
                return Err(BackendError::new(id, InitError(anyhow::Error::from(e))));
            }
        }

        Ok(Arc::new(Self::BackendImpl { id, root: ctx }))
    }

    async fn generate() -> anyhow::Result<BackendKind> {
        use inquire::validator::Validation;

        let path: String = inquire::CustomType::new("Dir path:")
            .with_validator(|s: &String| {
                let p: &Path = s.as_ref();
                if !p.is_dir() {
                    return Ok(Validation::Invalid("Path is not a dir".into()));
                }

                if p.read_dir()?.count() != 0 {
                    return Ok(Validation::Invalid("Dir is not empty".into()));
                }

                Ok(Validation::Valid)
            })
            .prompt()?;

        Ok(BackendKind::Dummy(path))
    }
}

pub struct DummyBackend {
    id: BackendId,
    root: String,
}

impl DummyBackend {
    /// Generates a path for a chunk with a given id
    fn path_for(&self, id: ChunkId) -> PathBuf {
        let root_path: &Path = self.root.as_ref();
        root_path.join(id.to_string())
    }
}

#[async_trait]
impl Backend for DummyBackend {
    async fn stat(&self) -> Result<BackendStat, BackendError<StatError>> {
        let err_map = |e: io::Error| BackendError::new(self.id, StatError(e.into()));

        let mut read_dir = fs::read_dir(&self.root).await.map_err(err_map)?;

        let mut total_size = 0;
        while let Some(result) = read_dir.next_entry().await.transpose() {
            let entry = result.map_err(err_map)?;
            let meta = entry.metadata().await.map_err(err_map)?;

            if !meta.is_file() {
                continue;
            }

            total_size += meta.size();
        }
        Ok(BackendStat {
            used: total_size,
            total: DEFAULT_CHUNK_SIZE as u64 * 100,
        })
    }

    async fn upload(&self, id: ChunkId, data: &[u8]) -> Result<(), BackendError<UploadError>> {
        let path = self.path_for(id);
        if fs::metadata(&path).await.is_ok() {
            return Err(BackendError::new(self.id, UploadError::ChunkDuplicate));
        }
        let mut file = fs::File::create_new(self.path_for(id))
            .await
            .context("Failed to create a new chunk file")
            .map_err(|e| BackendError::new(self.id, UploadError::Other(e)))?;

        match file.write(data).await {
            Ok(n) => {
                if n == data.len() {
                    Ok(())
                } else {
                    Err(BackendError::new(self.id, UploadError::OutOfSpace))
                }
            }
            Err(e) => Err(BackendError::new(self.id, UploadError::Other(e.into()))),
        }
    }

    async fn get(&self, id: ChunkId) -> Result<Vec<u8>, BackendError<GetError>> {
        fs::read(self.path_for(id))
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::NotFound => BackendError::new(self.id, GetError::ChunkNotFound),
                _ => BackendError::new(self.id, GetError::Other(e.into())),
            })
    }

    async fn delete(&self, id: ChunkId) -> Result<(), BackendError<DeleteError>> {
        fs::remove_file(self.path_for(id))
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::NotFound => BackendError::new(self.id, DeleteError::ChunkNotFound),
                _ => BackendError::new(self.id, DeleteError::Other(e.into())),
            })
    }
}

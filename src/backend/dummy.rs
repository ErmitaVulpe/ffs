use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use anyhow::Context;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
};

use super::*;

pub struct DummyBackend {
    backend_id: BackendId,
    root: String,
}

impl DummyBackend {
    pub async fn init(
        id: BackendId,
        path: String,
    ) -> Result<Box<dyn Backend>, BackendError<InitError>> {
        let result = fs::metadata(&path).await;
        match result {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(BackendError::new(
                        BackendKind::Dummy(path),
                        InitError(anyhow::anyhow!("Specified path is not a directory")),
                    ));
                }
            }
            Err(e) => {
                return Err(BackendError::new(
                    BackendKind::Dummy(path),
                    InitError(anyhow::Error::from(e)),
                ));
            }
        }

        Ok(Box::new(Self {
            backend_id: id,
            root: path,
        }))
    }

    fn path_for(&self, id: ChunkId) -> PathBuf {
        let root_path: &Path = self.root.as_ref();
        root_path.join(id.to_string())
    }
}

#[async_trait::async_trait]
impl Backend for DummyBackend {
    async fn stat(&self) -> Result<(u64, u64), BackendError<StatError>> {
        let err_map = |e: io::Error| {
            BackendError::new(BackendKind::Dummy(self.root.clone()), StatError(e.into()))
        };

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
        Ok((total_size, u64::MAX))
    }

    async fn upload(&self, id: ChunkId, data: &[u8]) -> Result<(), BackendError<UploadError>> {
        let path = self.path_for(id);
        if fs::metadata(&path).await.is_ok() {
            return Err(BackendError::new(
                BackendKind::Dummy(self.root.clone()),
                UploadError::ChunkDuplicate,
            ));
        }
        let mut file = fs::File::create_new(self.path_for(id))
            .await
            .context("Failed to create a new chunk file")
            .map_err(|e| {
                BackendError::new(BackendKind::Dummy(self.root.clone()), UploadError::Other(e))
            })?;

        match file.write(data).await {
            Ok(n) => {
                if n == data.len() {
                    Ok(())
                } else {
                    Err(BackendError::new(
                        BackendKind::Dummy(self.root.clone()),
                        UploadError::OutOfSpace,
                    ))
                }
            }
            Err(e) => Err(BackendError::new(
                BackendKind::Dummy(self.root.clone()),
                UploadError::Other(e.into()),
            )),
        }
    }

    async fn get(&self, id: ChunkId) -> Result<Vec<u8>, BackendError<GetError>> {
        fs::read(self.path_for(id))
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::NotFound => BackendError::new(
                    BackendKind::Dummy(self.root.clone()),
                    GetError::ChunkNotFound,
                ),
                _ => BackendError::new(
                    BackendKind::Dummy(self.root.clone()),
                    GetError::Other(e.into()),
                ),
            })
    }

    async fn delete(&self, id: ChunkId) -> Result<(), BackendError<DeleteError>> {
        fs::remove_file(self.path_for(id))
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::NotFound => BackendError::new(
                    BackendKind::Dummy(self.root.clone()),
                    DeleteError::ChunkNotFound,
                ),
                _ => BackendError::new(
                    BackendKind::Dummy(self.root.clone()),
                    DeleteError::Other(e.into()),
                ),
            })
    }
}

use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
};

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
                    return Err(BackendError::new(id, InitError::BackendRejected));
                }
            }
            Err(e) => {
                return Err(BackendError::new(
                    id,
                    InitError::Other(Arc::new(anyhow::Error::from(e))),
                ));
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
    fn path_for(&self, id: &BlobId) -> PathBuf {
        let root_path: &Path = self.root.as_ref();
        root_path.join(id.to_string())
    }
}

#[async_trait]
impl Backend for DummyBackend {
    async fn stat(&self) -> Result<BackendStat, BackendError<StatError>> {
        let err_map = |e: io::Error| self.error(StatError(e.into()));

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
            // 10GiB
            total: 10 * (1 << 30),
        })
    }

    fn id(&self) -> BackendId {
        self.id
    }

    async fn upload(&self, id: BlobId, data: &[u8]) -> Result<(), BackendError<UploadError>> {
        let stat = self
            .stat()
            .await
            .map_err(|e| self.error(UploadError::Other(Arc::new(e.into()))))?;
        if stat.used + data.len() as u64 > stat.total {
            return Err(self.error(UploadError::OutOfSpace));
        }

        let path = self.path_for(&id);
        if fs::metadata(&path).await.is_ok() {
            return Err(self.error(UploadError::BlobDuplicate));
        }
        let mut file = fs::File::create_new(self.path_for(&id))
            .await
            .context("Failed to create a new chunk file")
            .map_err(|e| self.error(UploadError::Other(Arc::new(e))))?;

        file.write_all(data)
            .await
            .map_err(|e| self.error(UploadError::Other(Arc::new(e.into()))))
    }

    async fn list(&self) -> Result<Vec<BlobId>, BackendError<ListError>> {
        let mut read_dir = fs::read_dir(&self.root)
            .await
            .map_err(|e| self.error(ListError::Other(Arc::new(e.into()))))?;
        let mut ids = Vec::new();

        loop {
            let res = read_dir
                .next_entry()
                .await
                .map_err(|e| self.error(ListError::Other(Arc::new(e.into()))))?;

            let Some(entry) = res else {
                break;
            };

            let raw_name = entry.file_name();
            let name = raw_name.to_string_lossy();
            let id = BlobId::from_str(&name)
                .map_err(|e| self.error(ListError::Other(Arc::new(e.into()))))?;
            ids.push(id);
        }

        Ok(ids)
    }

    async fn get(&self, id: BlobId) -> Result<Vec<u8>, BackendError<GetError>> {
        fs::read(self.path_for(&id))
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::NotFound => self.error(GetError::BlobNotFound),
                _ => self.error(GetError::Other(Arc::new(e.into()))),
            })
    }

    async fn delete(&self, id: BlobId) -> Result<(), BackendError<DeleteError>> {
        fs::remove_file(self.path_for(&id))
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::NotFound => self.error(DeleteError::BlobNotFound),
                _ => self.error(DeleteError::Other(Arc::new(e.into()))),
            })
    }
}

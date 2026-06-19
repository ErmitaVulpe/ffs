use derive_more::{Constructor, Display, Error, From};

use crate::db::{BackendId, BackendKind, ChunkId};

#[cfg(debug_assertions)]
mod dummy;

#[async_trait::async_trait]
pub trait Backend {
    /// Returns the tuple of used bytes and total storable bytes
    async fn stat(&self) -> Result<(u64, u64), BackendError<StatError>>;

    async fn upload(&self, id: ChunkId, data: &[u8]) -> Result<(), BackendError<UploadError>>;
    async fn get(&self, id: ChunkId) -> Result<Vec<u8>, BackendError<GetError>>;
    async fn delete(&self, id: ChunkId) -> Result<(), BackendError<DeleteError>>;
}

#[derive(Debug, Display, Error, From)]
#[display("Failed to read backend stats")]
pub struct StatError(#[error(source)] anyhow::Error);

#[derive(Constructor, Debug, Display, Error)]
#[display("Operation on backend of type \"{backend_kind}\" failed")]
pub struct BackendError<Kind> {
    pub backend_kind: BackendKind,
    #[error(source)]
    pub kind: Kind,
}

#[derive(Debug, Display, Error)]
pub enum UploadError {
    #[display("Backend ran out of space")]
    OutOfSpace,
    #[display("Chunk with this id already exists in the backend")]
    ChunkDuplicate,
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error)]
pub enum GetError {
    ChunkNotFound,
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error)]
pub enum DeleteError {
    ChunkNotFound,
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error, From)]
#[display("Failed to initialize this backend")]
pub struct InitError(#[error(source)] anyhow::Error);

pub async fn init(
    id: BackendId,
    backend_data: BackendKind,
) -> Result<Box<dyn Backend>, BackendError<InitError>> {
    match backend_data {
        #[cfg(debug_assertions)]
        BackendKind::Dummy(path) => dummy::DummyBackend::init(id, path).await,
    }
}

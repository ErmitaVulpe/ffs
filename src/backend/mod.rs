use std::sync::Arc;

use async_trait::async_trait;
use derive_more::{Constructor, Display, Error, From};

use crate::{
    BackendKindSpecifier,
    db::{BackendId, BackendKind, ChunkId},
};

#[cfg(debug_assertions)]
mod dummy;

/// Internal trait for implementing backends
#[async_trait]
trait BackendMod {
    type BackendImpl: Backend;
    type InitCtx;

    async fn init(id: BackendId, ctx: Self::InitCtx) -> Result<Arc<dyn Backend>, BackendError<InitError>>;
    async fn generate() -> anyhow::Result<BackendKind>;
}

#[async_trait]
pub trait Backend {
    /// Returns the tuple of used bytes and total storable bytes
    async fn stat(&self) -> Result<BackendStat, BackendError<StatError>>;

    async fn upload(&self, id: ChunkId, data: &[u8]) -> Result<(), BackendError<UploadError>>;
    async fn get(&self, id: ChunkId) -> Result<Vec<u8>, BackendError<GetError>>;
    async fn delete(&self, id: ChunkId) -> Result<(), BackendError<DeleteError>>;
}

pub struct BackendStat {
    pub used: u64,
    pub total: u64,
}

#[derive(Debug, Display, Error, From)]
#[display("Failed to read backend stats")]
pub struct StatError(#[error(source)] anyhow::Error);

#[derive(Constructor, Debug, Display, Error)]
#[display("Operation on backend with id {backend_id} failed")]
pub struct BackendError<Kind> {
    pub backend_id: BackendId,
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
) -> Result<Arc<dyn Backend>, BackendError<InitError>> {
    match backend_data {
        #[cfg(debug_assertions)]
        BackendKind::Dummy(path) => dummy::DummyImpl::init(id, path).await,
    }
}

/// This is used as an initial generator for init data of backends
pub async fn generate_backend(kind: BackendKindSpecifier) -> anyhow::Result<BackendKind> {
    match kind {
        BackendKindSpecifier::Dummy => dummy::DummyImpl::generate().await,
    }
}

use std::{convert::Infallible, fmt::Display, str::FromStr, sync::Arc};

use async_trait::async_trait;
use derive_more::{Constructor, Display, Error, From, IsVariant};
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::prelude::BackendId;

mod backend_ext;
#[cfg(debug_assertions)]
mod dummy;

pub use backend_ext::BackendExt;

#[derive(Debug, Display, Error)]
#[display("Unsupported backend kind")]
pub struct BackendParseError;

pub async fn init(
    id: BackendId,
    backend_data: BackendKind,
) -> Result<Arc<dyn Backend>, BackendError<InitError>> {
    match backend_data {
        #[cfg(debug_assertions)]
        BackendKind::Dummy(path) => dummy::DummyImpl::init(id, path).await,
        BackendKind::GoogleDrive => todo!(),
    }
}

/// This is used as an initial generator for init data of backends
pub async fn generate_backend(kind: BackendKindSpecifier) -> anyhow::Result<BackendKind> {
    match kind {
        #[cfg(debug_assertions)]
        BackendKindSpecifier::Dummy => dummy::DummyImpl::generate().await,
        BackendKindSpecifier::GoogleDrive => todo!(),
    }
}

/// Internal trait for implementing backends
#[async_trait]
trait BackendMod {
    type BackendImpl: Backend;
    type InitCtx;

    async fn init(
        id: BackendId,
        ctx: Self::InitCtx,
    ) -> Result<Arc<dyn Backend>, BackendError<InitError>>;
    async fn generate() -> anyhow::Result<BackendKind>;
}

#[async_trait]
pub trait Backend: Send + Sync {
    /// Returns the tuple of used bytes and total storable bytes
    async fn stat(&self) -> Result<BackendStat, BackendError<StatError>>;
    fn id(&self) -> BackendId;

    async fn upload(&self, id: BlobId, data: &[u8]) -> Result<(), BackendError<UploadError>>;
    async fn list(&self) -> Result<Vec<BlobId>, BackendError<ListError>>;
    async fn get(&self, id: BlobId) -> Result<Vec<u8>, BackendError<GetError>>;
    async fn delete(&self, id: BlobId) -> Result<(), BackendError<DeleteError>>;
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
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
    #[display("Blob with given id already exists in this backend")]
    BlobDuplicate,
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error)]
pub enum ListError {
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error)]
pub enum GetError {
    BlobNotFound,
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error)]
pub enum DeleteError {
    BlobNotFound,
    Other(anyhow::Error),
}

#[derive(Debug, Display, Error, From)]
#[display("Failed to initialize this backend")]
pub struct InitError(#[error(source)] anyhow::Error);

/// All data required to init a `dyn Backend`
#[derive(
    Archive, Serialize, Deserialize, Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord,
)]
#[repr(u8)]
pub enum BackendKind {
    /// Dummy backend which stores chunks as files at the given path
    #[cfg(debug_assertions)]
    #[display("Dummy")]
    Dummy(String) = 0,
    #[display("GoogleDrive")]
    GoogleDrive = 1,
}

/// Just a specifier for the backend kind
#[derive(Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BackendKindSpecifier {
    /// Dummy backend which stores chunks as files at the given path
    #[cfg(debug_assertions)]
    #[display("Dummy")]
    Dummy = 0,
    #[display("GoogleDrive")]
    GoogleDrive = 1,
}

impl FromStr for BackendKindSpecifier {
    type Err = BackendParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let a = s.to_lowercase();
        Ok(match a.as_str() {
            #[cfg(debug_assertions)]
            "dummy" => Self::Dummy,
            "google" | "googledrive" => Self::GoogleDrive,
            _ => return Err(BackendParseError),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, IsVariant)]
pub enum BlobId {
    /// Data blob for storage backends
    Uuid(Uuid),

    /// Meta dir entry for the bootstrap
    Meta(u64),
    /// Lease dir entry for the bootstrap
    Lease(Uuid),

    /// Other (invalid) blob name
    Other(String),
}

impl FromStr for BlobId {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let opt = (|| {
            if let Ok(uuid) = Uuid::from_str(s) {
                return Some(Self::Uuid(uuid));
            }

            let (pref, suff) = s.split_once('.')?;

            match pref {
                "m" => {
                    let rev = u64::from_str(suff).ok()?;
                    Some(Self::Meta(rev))
                }
                "l" => {
                    let uuid = Uuid::from_str(suff).ok()?;
                    Some(Self::Lease(uuid))
                }
                _ => None,
            }
        })();

        Ok(opt.unwrap_or(Self::Other(s.to_string())))
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobId::Uuid(uuid) => write!(f, "{uuid}"),
            BlobId::Meta(rev) => write!(f, "m.{rev}"),
            BlobId::Lease(uuid) => write!(f, "l.{uuid}"),
            BlobId::Other(s) => write!(f, "{s}"),
        }
    }
}

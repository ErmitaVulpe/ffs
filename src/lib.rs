use std::{collections::BTreeMap, path::Path, sync::Arc};

use anyhow::Context;
use tempfile::TempDir;
use tokio::{fs, io::AsyncWriteExt, sync::RwLock};
use uuid::Uuid;

use crate::{
    backend::{ArchivedBackendKind, Backend, BackendExt, BackendKindSpecifier, BlobId},
    state::{Lease, LeaseEntry, State},
};

mod backend;
pub mod prelude;
mod state;

#[derive(Clone)]
pub struct App {
    inner: Arc<AppInner>,
}

impl App {
    /// The normal way of starting the App, from an already exisitng bootstrap
    pub async fn new(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(AppInner::new(bootstrap_path).await?),
        })
    }

    /// Used to create a new bootstrap file
    pub async fn init(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(AppInner::init(bootstrap_path).await?),
        })
    }
}

struct AppInner {
    bootstrap: Arc<dyn Backend>,
    state: RwLock<AppState>,
    tempdir: TempDir,
}

impl AppInner {
    async fn new(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let tempdir = TempDir::new()?;

        let base58 = fs::read_to_string(bootstrap_path)
            .await
            .context("Failed to read botstrap data")?;
        let bytes = bs58::decode(&base58)
            .into_vec()
            .context("Failed to decode bootstrap data")?;
        let archived = rkyv::access::<ArchivedBackendKind, rkyv::rancor::Error>(&bytes)
            .context("Failed to access bootstrap data")?;
        let backend_data = rkyv::deserialize::<_, rkyv::rancor::Error>(archived)
            .context("Failed to deserialize bootstrap data")?;

        let bootstrap = backend::init(0, backend_data).await?;
        let state_bytes = bootstrap.get_latest_state().await?;
        let confirmed_state = rkyv::from_bytes::<State, rkyv::rancor::Error>(&state_bytes)?;

        Ok(AppInner {
            bootstrap,
            state: RwLock::new(AppState::new(confirmed_state)),
            tempdir,
        })
    }

    async fn init(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if fs::metadata(&bootstrap_path).await.is_ok() {
            anyhow::bail!("Bootstrap file at path already exists");
        }

        let kind_spec: BackendKindSpecifier = inquire::Select::new(
            "What backend type should the bootstrap be?",
            vec![
                #[cfg(debug_assertions)]
                "dummy",
                "google",
            ],
        )
        .prompt()?
        .parse()?;

        let backend_data = backend::generate_backend(kind_spec).await?;
        let bootstrap = backend::init(0, backend_data.clone()).await?;

        let initial_state = State::default();
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&initial_state)?;
        bootstrap.upload(BlobId::Meta(0), &bytes).await?;

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&backend_data)?;
        let base58 = bs58::encode(&bytes).into_string();
        let mut file = fs::File::create_new(bootstrap_path).await?;
        file.write_all(base58.as_bytes()).await?;

        Ok(AppInner {
            bootstrap,
            state: RwLock::new(AppState::new(initial_state)),
            tempdir: TempDir::new()?,
        })
    }

    async fn refresh_leases(self: &Arc<Self>) -> Result<(), anyhow::Error> {
        let a = self.bootstrap.get_all_leases().await?;

        todo!()
    }
}

#[derive(Debug)]
struct AppState {
    confirmed: State,
    local: State,
    active_leases: ActiveLeases,
}

impl AppState {
    fn new(confirmed_state: State) -> Self {
        Self {
            local: confirmed_state.new_child(),
            confirmed: confirmed_state,
            active_leases: ActiveLeases::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ActiveLeases {
    inner: BTreeMap<Uuid, Lease>,
}

impl ActiveLeases {
    fn all_entries(&self) -> impl Iterator<Item = &LeaseEntry> {
        self.inner.iter().flat_map(|l| &l.1.blobs)
    }
}

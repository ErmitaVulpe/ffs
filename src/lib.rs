use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{RwLock, watch},
};
use uuid::Uuid;

use crate::{
    app_arc::AppArc,
    backend::{ArchivedBackendKind, Backend, BackendExt, BackendKindSpecifier, BlobId},
    state::{Lease, LeaseEntry, State},
    state_manager::StateManagerHandle,
};

mod app_arc;
mod backend;
pub mod prelude;
mod state;
mod state_manager;

#[derive(Clone)]
pub struct App {
    inner: Arc<AppInner>,
}

impl App {
    /// The normal way of starting the App, from an already exisitng bootstrap
    pub async fn new(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: AppInner::new(bootstrap_path).await?,
        })
    }

    /// Used to create a new bootstrap file
    pub async fn init(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: AppInner::init(bootstrap_path).await?,
        })
    }
}

struct AppInner {
    bootstrap: Arc<dyn Backend>,
    state: AppState,
    tempdir: TempDir,
}

impl AppInner {
    fn create_helper(
        bootstrap: Arc<dyn Backend>,
        confirmed_state: State,
    ) -> anyhow::Result<Arc<Self>> {
        let tempdir = TempDir::new()?;
        let (watch_tx, watch_rx) = watch::channel(());

        let arc = Arc::new_cyclic(|weak| {
            let app_arc = AppArc::new(weak.to_owned(), watch_rx);

            AppInner {
                bootstrap,
                state: AppState::new(confirmed_state, app_arc),
                tempdir,
            }
        });

        let _ = watch_tx.send(());
        Ok(arc)
    }

    async fn new(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
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

        let app_inner = Self::create_helper(bootstrap, confirmed_state)?;
        app_inner.refresh_leases().await?;

        Ok(app_inner)
    }

    async fn init(bootstrap_path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
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

        Self::create_helper(bootstrap, initial_state)
    }

    async fn refresh_leases(self: &Arc<Self>) -> Result<(), anyhow::Error> {
        let confirmed_ids = self
            .bootstrap
            .get_all_lease_ids()
            .await?
            .collect::<BTreeSet<_>>();
        let active_ids = self
            .state
            .active_leases
            .read()
            .await
            .inner
            .keys()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();

        let ids_to_drop = active_ids.difference(&confirmed_ids).collect::<Vec<_>>();
        let ids_to_add = confirmed_ids
            .difference(&active_ids)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        if ids_to_drop.is_empty() && ids_to_add.is_empty() {
            return Ok(());
        }

        let leases_to_add = self
            .bootstrap
            .get_leases(ids_to_add.into_iter())
            .await?
            .into_iter()
            .map(|(id, buf)| {
                rkyv::from_bytes::<Lease, rkyv::rancor::Error>(&buf).map(|lease| (id, lease))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        let mut lease_lock = self.state.active_leases.write().await;
        for id in ids_to_drop {
            lease_lock.inner.remove(id);
        }
        for (id, lease) in leases_to_add {
            lease_lock.inner.insert(id, lease);
        }

        Ok(())
    }
}

#[derive(Debug)]
struct AppState {
    local: RwLock<State>,
    active_leases: RwLock<ActiveLeases>,
    state_manager: StateManagerHandle,
}

impl AppState {
    fn new(confirmed_state: State, app_arc: AppArc) -> Self {
        let local = RwLock::new(confirmed_state.new_child());
        Self {
            local,
            active_leases: RwLock::new(ActiveLeases::default()),
            state_manager: StateManagerHandle::init(confirmed_state, app_arc),
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

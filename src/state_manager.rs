use std::{
    collections::{BTreeMap, BTreeSet},
    future::pending,
    sync::Arc,
    time::Duration,
};

use derive_more::{Display, Error, From, IsVariant};
use tokio::{
    fs::File,
    io::{self, AsyncReadExt, AsyncSeekExt},
    spawn,
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinHandle, JoinSet},
};

use crate::{
    AppInner, GetBackendError,
    app_arc::AppArc,
    backend::{BackendError, BackendExt, BlobId, SetStateError, UploadError},
    state::State,
};

const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct StateManagerHandle {
    handle: JoinHandle<()>,
    command_tx: mpsc::Sender<ManagerCommand>,
    manager_state_rx: watch::Receiver<ManagerState>,
}

impl Drop for StateManagerHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl StateManagerHandle {
    pub fn init(confirmed_state: State, app_arc: AppArc) -> Self {
        let (manager_state_tx, manager_state_rx) =
            watch::channel(ManagerState::new(confirmed_state));
        let (command_tx, start_commit_rx) = mpsc::channel(4);

        Self {
            handle: spawn(state_manager(app_arc, start_commit_rx, manager_state_tx)),
            command_tx,
            manager_state_rx,
        }
    }

    pub fn send(
        &self,
        command: ManagerCommand,
    ) -> impl Future<Output = Result<(), mpsc::error::SendError<ManagerCommand>>> {
        self.command_tx.send(command)
    }

    pub fn watch_manager_state(&self) -> watch::Receiver<ManagerState> {
        self.manager_state_rx.clone()
    }
}

async fn state_manager(
    app_arc: AppArc,
    mut start_commit_rx: mpsc::Receiver<ManagerCommand>,
    manager_state_tx: watch::Sender<ManagerState>,
) {
    let Ok(app) = app_arc.upgrade().await else {
        return;
    };

    let mut timer = None;
    let arm_timer = || Some(Box::pin(tokio::time::sleep(COMMIT_TIMEOUT)));
    let mut commit_task = None;
    let mut state = manager_state_tx.borrow().clone();

    let mut current_listeners = Vec::new();
    let mut queued_listeners = Vec::new();

    loop {
        tokio::select! {
            biased;

            // Handling commands from other threads
            Some(command) = start_commit_rx.recv() => {
                match state.commiting_state.is_some() {
                    false => {
                        if let Some(resp) = command.result_return {
                            current_listeners.push(resp);
                        }

                        if command.kind.is_start_now() {
                            timer = None;
                            let state_to_commit = app.state.local.read().await.clone();
                            state.commiting_state = Some(state_to_commit.clone());
                            commit_task = Some(spawn(commit(app.clone(), state_to_commit)));
                        } else if !state.commit_is_pending {
                            state.commit_is_pending = true;
                            timer = arm_timer();
                        }

                        let _ = manager_state_tx.send(state.clone());
                    }
                    true => {
                        if !state.commit_is_pending {
                            state.commit_is_pending = true;
                            let _ = manager_state_tx.send(state.clone());
                        }

                        if let Some(resp) = command.result_return {
                            queued_listeners.push(resp);
                        }
                    }
                }
            }

            // Commit task completion
            res = async {
                if let Some(task) = &mut commit_task {
                    task.await
                } else {
                    pending::<Result<Result<(), CommitError>, JoinError>>().await
                }
            }, if commit_task.is_some() => {
                let res = res.expect("The commit task got aborted");

                app.state
                    .local
                    .write()
                    .await
                    .update_by_commited(state.commiting_state.as_ref().unwrap());

                if state.commit_is_pending {
                    state.commit_is_pending = false;
                    let state_to_commit = app.state.local.read().await.clone();
                    state.commiting_state = Some(state_to_commit.clone());
                    commit_task = Some(spawn(commit(app.clone(), state_to_commit)));
                }

                let _ = manager_state_tx.send(state.clone());

                for tx in current_listeners.drain(..) {
                    let _ = tx.send(res.clone());
                }
            }

            // Commit timer firing
            _ = async {
                if let Some(timer) = &mut timer {
                    timer.await;
                } else {
                    pending::<()>().await;
                }
            }, if timer.is_some() => {
                timer = None;
                let state_to_commit = app.state.local.read().await.clone();
                state.commiting_state = Some(state_to_commit.clone());
                commit_task = Some(spawn(commit(app.clone(), state_to_commit)));
            }
        }
    }
}

pub struct ManagerCommand {
    kind: ManagerCommandKind,
    result_return: Option<oneshot::Sender<Result<(), CommitError>>>,
}

#[derive(IsVariant)]
enum ManagerCommandKind {
    Enqueue,
    StartNow,
}

impl ManagerCommand {
    pub fn enqueue(resp: Option<oneshot::Sender<Result<(), CommitError>>>) -> Self {
        Self {
            kind: ManagerCommandKind::Enqueue,
            result_return: resp,
        }
    }

    pub fn start_now(resp: Option<oneshot::Sender<Result<(), CommitError>>>) -> Self {
        Self {
            kind: ManagerCommandKind::StartNow,
            result_return: resp,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ManagerState {
    pub confirmed_state: State,
    commit_is_pending: bool,
    commiting_state: Option<State>,
}

impl ManagerState {
    fn new(confirmed_state: State) -> Self {
        Self {
            confirmed_state,
            commit_is_pending: false,
            commiting_state: None,
        }
    }

    pub fn commiting_state<'a>(&'a self) -> ManagerCommitState<'a> {
        match (&self.commiting_state, self.commit_is_pending) {
            (None, false) => ManagerCommitState::Idle,
            (None, true) => ManagerCommitState::Pending,
            (Some(val), false) => ManagerCommitState::Commiting(val),
            (Some(val), true) => ManagerCommitState::CommitingAndQueued(val),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ManagerCommitState<'a> {
    Idle,
    Pending,
    Commiting(&'a State),
    CommitingAndQueued(&'a State),
}

// TODO This function should include automatic gc run when out of space
async fn commit(app: Arc<AppInner>, state: State) -> Result<(), CommitError> {
    let upload_plan = state.upload_plan();

    let lease_id = app
        .bootstrap
        .set_lease(&upload_plan.lease)
        .await
        .map_err(CommitError::AddLease)?;
    app.state
        .active_leases
        .write()
        .await
        .inner
        .insert(lease_id, upload_plan.lease);

    let required_backends = upload_plan
        .actions
        .iter()
        .map(|b| b.locator.backend_id)
        .collect::<BTreeSet<_>>();

    let mut backends = BTreeMap::new();
    for backend_id in required_backends {
        let backend = app.get_backend(backend_id).await?;
        backends.insert(backend_id, backend);
    }

    let mut join_set = JoinSet::from_iter(upload_plan.actions.iter().cloned().map(|b| {
        let app = app.clone();
        let backend = backends.get(&b.locator.backend_id).unwrap().clone();

        async move {
            let mut file = File::open(app.tempdir.path().join(b.source_file.to_string()))
                .await
                .map_err(CommitError::no_source_file)?;
            file.seek(std::io::SeekFrom::Start(b.offset)).await?;
            let mut buf = vec![0u8; b.length as usize];
            file.read_exact(&mut buf).await?;
            backend
                .upload(BlobId::Uuid(b.locator.uuid), &buf)
                .await
                .map_err(CommitError::ChunkUpload)?;

            Ok(())
        }
    }));

    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res.expect("Tasks are never cancelled") {
            join_set.shutdown().await;
            return Err(err);
        }
    }

    app.bootstrap.set_state(&state).await?;
    Ok(())
}

#[derive(Clone, Debug, Display, Error, From)]
pub enum CommitError {
    #[display("Failed to initialize a backend")]
    #[from]
    GetBackend(GetBackendError),
    #[display("Failed to register a new lease")]
    #[from(skip)]
    AddLease(BackendError<UploadError>),
    #[display("Tried to upload data from a non existing file")]
    #[from(skip)]
    NoSourceFile(Arc<io::Error>),
    #[display("Failed to upload a chunk")]
    #[from(skip)]
    ChunkUpload(BackendError<UploadError>),
    #[display("Failed to upload local state")]
    #[from]
    SetState(SetStateError),
    #[from(io::Error)]
    Io(Arc<io::Error>),
}

impl CommitError {
    fn no_source_file(e: io::Error) -> Self {
        Self::NoSourceFile(Arc::new(e))
    }
}

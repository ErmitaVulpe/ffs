use std::{future::pending, sync::Arc, time::Duration};

use derive_more::{Display, Error, IsVariant};
use tokio::{
    spawn,
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};

use crate::{AppInner, app_arc::AppArc, state::State};

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

                // TODO shrink the new local state changes to fit the new confirmed

                for tx in current_listeners.drain(..) {
                    let _ = tx.send(res.clone());
                }

                if state.commit_is_pending {
                    state.commit_is_pending = false;
                    let state_to_commit = app.state.local.read().await.clone();
                    state.commiting_state = Some(state_to_commit.clone());
                    commit_task = Some(spawn(commit(app.clone(), state_to_commit)));
                }

                let _ = manager_state_tx.send(state.clone());
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

// TODO This function should include automatic retries
async fn commit(app: Arc<AppInner>, state: State) -> Result<(), CommitError> {
    todo!()
}

#[derive(Clone, Debug, Display, Error)]
pub enum CommitError {}

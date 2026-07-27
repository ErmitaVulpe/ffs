use std::time::Duration;

use tokio::{spawn, sync::watch, task::JoinHandle};

use crate::{app_arc::AppArc, state::State};

const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct StateManagerHandle {
    handle: JoinHandle<()>,
    confirmed_rx: watch::Receiver<State>,
    commiting_rx: watch::Receiver<Option<State>>,
    commiting_result_rx: watch::Receiver<Result<(), anyhow::Error>>,
    start_commit_tx: watch::Sender<()>,
}

impl Drop for StateManagerHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl StateManagerHandle {
    pub fn init(confirmed_state: State, app_arc: AppArc) -> Self {
        let (confirmed_tx, confirmed_rx) = watch::channel(confirmed_state);
        let (commiting_tx, commiting_rx) = watch::channel(None);
        let (commiting_result_tx, commiting_result_rx) = watch::channel(Ok(()));
        let (start_commit_tx, start_commit_rx) = watch::channel(());

        Self {
            handle: spawn(state_manager(
                app_arc,
                confirmed_tx,
                commiting_tx,
                commiting_result_tx,
                start_commit_rx,
            )),
            confirmed_rx,
            commiting_rx,
            commiting_result_rx,
            start_commit_tx,
        }
    }
}

async fn state_manager(
    app_arc: AppArc,
    confirmed_tx: watch::Sender<State>,
    commiting_tx: watch::Sender<Option<State>>,
    commiting_result_tx: watch::Sender<Result<(), anyhow::Error>>,
    mut start_commit_rx: watch::Receiver<()>,
) {
    let Ok(app) = app_arc.upgrade().await else {
        return;
    };

    let mut timer = None;

    loop {
        tokio::select! {
            biased;

            Ok(()) = start_commit_rx.changed() => {
                timer = Some(Box::pin(tokio::time::sleep(COMMIT_TIMEOUT)));
            }

            _ = async {
                if let Some(timer) = &mut timer {
                    timer.await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if timer.is_some() => {
                timer = None;

                let state_to_commit = app.state.local.read().await.clone();
                let _ = commiting_tx.send(Some(state_to_commit.clone()));

                // Start commiting state
            }
        }
    }
}

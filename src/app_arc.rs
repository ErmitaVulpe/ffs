use std::sync::{Arc, Weak};

use derive_more::Constructor;
use tokio::sync::watch;

use crate::AppInner;

#[derive(Constructor, Clone)]
pub struct AppArc {
    weak: Weak<AppInner>,
    completion_rx: watch::Receiver<()>,
}

impl AppArc {
    pub async fn upgrade(mut self) -> Result<Arc<AppInner>, watch::error::RecvError> {
        self.completion_rx.changed().await?;
        Ok(self.weak.upgrade().unwrap())
    }
}

use std::collections::BTreeMap;

use async_trait::async_trait;
use derive_more::{Display, Error, From};
use indoc::indoc;
use tokio::task::JoinSet;

use crate::state::State;

use super::*;

/// Helper impls for the `Backend`. Should not be manually implemented, unless
/// it is notably more efficient
#[async_trait]
pub trait BackendExt: Backend
where
    Self: 'static,
{
    fn error<T>(&self, kind: T) -> BackendError<T> {
        BackendError::new(self.id(), kind)
    }

    //
    // --- Getting State
    //
    async fn get_state(&self, rev: u64) -> Result<Vec<u8>, BackendError<GetError>> {
        self.get(BlobId::Meta(rev)).await
    }

    /// returned revs are in random order
    async fn get_state_revs(&self) -> Result<impl Iterator<Item = u64>, BackendError<ListError>> {
        Ok(self.list().await?.into_iter().filter_map(|id| match id {
            BlobId::Meta(rev) => Some(rev),
            _ => None,
        }))
    }

    async fn get_latest_state(&self) -> Result<Vec<u8>, GetLatestStateError> {
        let latest_rev = self
            .get_state_revs()
            .await?
            .max()
            .ok_or(GetLatestStateError::NoState)?;
        Ok(self.get_state(latest_rev).await?)
    }

    //
    // --- Setting State
    //
    async fn set_state(&self, state: &State) -> Result<(), BackendError<UploadError>> {
        let rev = state.rev();
        let buf = rkyv::to_bytes::<rkyv::rancor::Error>(state).unwrap();
        self.upload(BlobId::Meta(rev), &buf).await
    }

    //
    // --- Getting leases
    //

    /// returns ids of all the leases listed in the bootstrap
    async fn get_all_lease_ids(
        &self,
    ) -> Result<impl Iterator<Item = Uuid> + Send, BackendError<ListError>> {
        Ok(self.list().await?.into_iter().filter_map(|id| match id {
            BlobId::Lease(id) => Some(id),
            _ => None,
        }))
    }

    async fn get_lease(&self, id: Uuid) -> Result<Vec<u8>, BackendError<GetError>> {
        self.get(BlobId::Lease(id)).await
    }

    async fn get_leases(
        self: &Arc<Self>,
        lease_ids: impl Iterator<Item = Uuid> + Send,
    ) -> Result<BTreeMap<Uuid, Vec<u8>>, BackendError<ListError>> {
        let mut join_set = lease_ids
            .map(|id| (id, self.clone()))
            .map(|(id, here_self)| async move {
                here_self.get(BlobId::Lease(id)).await.map(|buf| (id, buf))
            })
            .collect::<JoinSet<_>>();
        let mut output = BTreeMap::new();

        while let Some(join_res) = join_set.join_next().await {
            let res = join_res.expect(indoc! {"
                If this is an error, it means that either the get task was \
                cancelled or panicked, none of which should be possible
            "});

            match res {
                Ok((id, buf)) => {
                    let res = output.insert(id, buf);
                    debug_assert!(res.is_none(), "Duplicate lease ids");
                }
                Err(err) => match err.kind {
                    GetError::BlobNotFound => continue,
                    GetError::Other(error) => return Err(self.error(ListError::Other(error))),
                },
            }
        }

        Ok(output)
    }

    async fn get_all_leases(
        self: &Arc<Self>,
    ) -> Result<BTreeMap<Uuid, Vec<u8>>, BackendError<ListError>> {
        self.get_leases(self.get_all_lease_ids().await?).await
    }
}

impl<T: Backend + ?Sized + 'static> BackendExt for T {}

#[derive(Debug, Display, Error, From)]
pub enum GetLatestStateError {
    GetError(BackendError<GetError>),
    ListError(BackendError<ListError>),
    #[display("No state revision found")]
    NoState,
}

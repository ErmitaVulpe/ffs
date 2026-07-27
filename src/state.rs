use std::{
    collections::{BTreeMap, btree_map},
    time::{SystemTime, UNIX_EPOCH},
};

use derive_more::{Constructor, Deref, DerefMut, Display, Error, IsVariant};
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    backend::{BackendKind, BackendStat, BlobId},
    prelude::InodePath,
};

/// 6 hours
pub const LEASE_DURATION: u64 = 6 * 3600;

pub type BackendId = u32;
pub type Result<T, E = StateError> = std::result::Result<T, E>;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct State {
    /// The revision number as visible in the bootstrap
    rev: u64,
    file_tree: FileTree,
    backends: BTreeMap<BackendId, BackendMeta>,
    /// A list of changes that when replayed will transform the previous
    /// revision into the current one
    performed_changes: Vec<StateChange>,
}

impl State {
    pub fn new_child(&self) -> Self {
        Self {
            rev: self.rev.checked_add(1).expect("Ran out of revision ids"),
            performed_changes: Vec::new(),
            ..self.clone()
        }
    }

    pub fn rev(&self) -> u64 {
        self.rev
    }

    pub fn perform_change(&mut self, change: StateChange) -> Result<()> {
        match &change {
            StateChange::AddedBackend(meta) => {
                let new_id = self
                    .backends
                    .last_key_value()
                    .map(|(k, _)| k.checked_add(1).ok_or(StateError::OutOfBackendIds))
                    .transpose()?
                    .unwrap_or_default();
                self.backends.insert(new_id, meta.to_owned());
            }
            StateChange::RemovedBackend(backend_id) => {
                let entry = match self.backends.entry(*backend_id) {
                    btree_map::Entry::Vacant(_) => return Err(StateError::BackendNotFound),
                    btree_map::Entry::Occupied(val) => val,
                };

                if entry.get().stat.used != 0 {
                    return Err(StateError::BackendNotEmpty);
                }

                entry.remove();
            }
            StateChange::UploadFile {
                source_file: _,
                path,
                file_inode,
            } => {
                let file_entry = match self.file_tree.resolve_path_entry(path)? {
                    btree_map::Entry::Vacant(entry) => entry,
                    btree_map::Entry::Occupied(_) => return Err(StateError::FileExists),
                };

                // Try to apply the extents to backends
                let mut backends_copy = self.backends.clone();
                for extent in &file_inode.extents {
                    let backend_meta = backends_copy
                        .get_mut(&extent.locator.backend_id)
                        .ok_or(StateError::BackendNotFound)?;

                    let new_used = backend_meta
                        .stat
                        .used
                        .checked_add(extent.length)
                        .ok_or(StateError::BackendOutOfSpace)?;

                    if new_used > backend_meta.stat.total {
                        return Err(StateError::BackendOutOfSpace);
                    }

                    backend_meta.stat.used = new_used;
                }

                // All checks passed. Now modifying the state
                file_entry.insert(FileTreeEntry::File(file_inode.to_owned()));
                self.backends = backends_copy;
            }
            StateChange::RemoveFile(path) => {
                let tree_entry = match self.file_tree.resolve_path_entry(path)? {
                    btree_map::Entry::Vacant(_) => return Err(StateError::FileNotFound),
                    btree_map::Entry::Occupied(val) => val,
                };

                let file_inode = match tree_entry.get() {
                    FileTreeEntry::File(val) => val,
                    _ => return Err(StateError::PathNotAFile),
                };

                // Check if no backend has negative used now
                let mut backends_copy = self.backends.clone();
                for extent in &file_inode.extents {
                    let backend_meta = backends_copy
                        .get_mut(&extent.locator.backend_id)
                        .ok_or(StateError::InvalidState)?;

                    let new_used = backend_meta
                        .stat
                        .used
                        .checked_sub(extent.length)
                        .ok_or(StateError::InvalidState)?;

                    backend_meta.stat.used = new_used;
                }

                // All checks passed. Now modifying the state
                tree_entry.remove();
                self.backends = backends_copy;
            }
        }

        self.performed_changes.push(change);

        Ok(())
    }

    pub fn upload_plan(&self) -> UploadPlan {
        let mut lease = Lease::new();
        let mut actions = Vec::new();

        for change in &self.performed_changes {
            if let StateChange::UploadFile { // TODO Check for removes to remove the files from
                                             // actions etc
                source_file,
                path: _,
                file_inode,
            } = change
            {
                let mut offset = 0;
                for extent in &file_inode.extents {
                    lease
                        .blobs
                        .push(LeaseEntry::new(extent.locator.clone(), extent.length));
                    actions.push(UploadBlob::new(
                        extent.locator.clone(),
                        *source_file,
                        offset,
                        extent.length,
                    ));
                    offset += extent.length;
                }
            }
        }

        UploadPlan { lease, actions }
    }
}

#[derive(Debug, Display, Error, IsVariant)]
pub enum StateError {
    #[display("This state is corrupted")]
    InvalidState,
    #[display("Ran out of backend ids")]
    OutOfBackendIds,
    #[display("Speified backend does not exist or was recently removed")]
    BackendNotFound,
    #[display("Speified backend not empty")]
    BackendNotEmpty,
    #[display("Backend can't hold the required ammount of new data")]
    BackendOutOfSpace,
    #[display("Specified file path contained no filename")]
    NoFilenameGiven,
    #[display("Supplied path is empty")]
    PathEmpty,
    #[display("Supplied file path is broken")]
    BrokenPath,
    #[display("Path is not a file")]
    PathNotAFile,
    #[display("File with this name already exists")]
    FileExists,
    #[display("Specified file doesn't exist")]
    FileNotFound,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default, Deref, DerefMut)]
struct FileTree(BTreeMap<String, FileTreeEntry>);

impl FileTree {
    fn resolve_path(&self, path: &InodePath) -> Result<&FileTreeEntry> {
        let mut cur_dir = self;
        let mut iter = path.iter();
        let len = iter.len();
        for seg in iter.by_ref().take(len.saturating_sub(1)) {
            let next_entry = cur_dir.get(seg).ok_or(StateError::BrokenPath)?;
            match next_entry {
                FileTreeEntry::Dir(btree_map) => cur_dir = btree_map,
                FileTreeEntry::File(_) => return Err(StateError::BrokenPath),
            }
        }

        cur_dir
            .get(iter.next().ok_or(StateError::PathEmpty)?)
            .ok_or(StateError::BrokenPath)
    }

    fn resolve_path_mut(&mut self, path: &InodePath) -> Result<&mut FileTreeEntry> {
        let mut cur_dir = self;
        let mut iter = path.iter();
        let len = iter.len();
        for seg in iter.by_ref().take(len.saturating_sub(1)) {
            let next_entry = cur_dir.get_mut(seg).ok_or(StateError::BrokenPath)?;
            match next_entry {
                FileTreeEntry::Dir(btree_map) => cur_dir = btree_map,
                FileTreeEntry::File(_) => return Err(StateError::BrokenPath),
            }
        }

        cur_dir
            .get_mut(iter.next().ok_or(StateError::PathEmpty)?)
            .ok_or(StateError::BrokenPath)
    }

    fn resolve_path_entry(
        &mut self,
        path: &InodePath,
    ) -> Result<btree_map::Entry<'_, String, FileTreeEntry>> {
        let mut cur_dir = self;
        let mut iter = path.iter();
        let len = iter.len();
        for seg in iter.by_ref().take(len.saturating_sub(1)) {
            let next_entry = cur_dir.get_mut(seg).ok_or(StateError::BrokenPath)?;
            match next_entry {
                FileTreeEntry::Dir(btree_map) => cur_dir = btree_map,
                FileTreeEntry::File(_) => return Err(StateError::BrokenPath),
            }
        }

        Ok(cur_dir.entry(iter.next().ok_or(StateError::PathEmpty)?.to_owned()))
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
enum FileTreeEntry {
    Dir(#[rkyv(omit_bounds)] FileTree),
    File(FileInode),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct FileInode {
    length: u64,
    extents: Vec<Extent>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Extent {
    length: u64,
    locator: BlobLocator,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobLocator {
    backend_id: BackendId,
    uuid: Uuid,
}

impl BlobLocator {
    fn into_blob_id(self) -> (BackendId, BlobId) {
        let Self { backend_id, uuid } = self;
        (backend_id, BlobId::Uuid(uuid))
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct BackendMeta {
    pub kind: BackendKind,
    /// This includes only confirmed, used bytes and not ones taken up by leases
    pub stat: BackendStat,
}

#[non_exhaustive]
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum StateChange {
    AddedBackend(BackendMeta),
    RemovedBackend(BackendId),
    UploadFile {
        source_file: Uuid,
        path: InodePath,
        file_inode: FileInode,
    },
    RemoveFile(InodePath),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct Lease {
    valid_until_unix: u64,
    pub blobs: Vec<LeaseEntry>,
}

impl Lease {
    pub fn new() -> Self {
        let curr = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs();

        Self {
            valid_until_unix: curr + LEASE_DURATION,
            blobs: Vec::new(),
        }
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Constructor)]
pub struct LeaseEntry {
    pub locator: BlobLocator,
    pub size: u64,
}

pub struct UploadPlan {
    lease: Lease,
    actions: Vec<UploadBlob>,
}

#[derive(Constructor, Debug)]
pub struct UploadBlob {
    locator: BlobLocator,
    source_file: Uuid,
    offset: u64,
    length: u64,
}

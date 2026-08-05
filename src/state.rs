use std::{
    collections::{BTreeMap, btree_map},
    time::{SystemTime, UNIX_EPOCH},
};

use derive_more::{Constructor, Deref, DerefMut, Display, Error, IsVariant};
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    backend::{BackendKind, BackendStat},
    prelude::{BackendId, InodePath},
};

/// 6 hours
pub const LEASE_DURATION: u64 = 6 * 3600;

pub type Result<T, E = StateError> = std::result::Result<T, E>;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct State {
    /// The revision number as visible in the bootstrap
    rev: u64,
    file_tree: FileTree,
    backends: BTreeMap<BackendId, BackendMeta>,
    /// A list of changes that when replayed will transform the previous
    /// revision into the current one. Even if a new change undoes an existing
    /// change, it should not change previous ones
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

    pub fn get_backend(&self, id: BackendId) -> Option<&BackendMeta> {
        self.backends.get(&id)
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
                let file_entry = match self.file_tree.resolve_dir_entry(path.as_slice())? {
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
                let tree_entry = match self.file_tree.resolve_dir_entry(path.as_slice())? {
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
        let mut uploading_files = BTreeMap::new();

        for change in &self.performed_changes {
            match change {
                StateChange::UploadFile {
                    source_file,
                    path,
                    file_inode,
                } => {
                    let new_blobs = file_inode
                        .extents
                        .iter()
                        .scan(0, |offset, extent| {
                            let ret = Some(UploadBlob::new(
                                extent.locator.clone(),
                                *source_file,
                                *offset,
                                extent.length,
                            ));
                            *offset += extent.length;
                            ret
                        })
                        .collect::<Vec<_>>();

                    let res = uploading_files.insert(path, new_blobs);
                    debug_assert!(res.is_none());
                }
                StateChange::RemoveFile(inode_path) => {
                    uploading_files.remove(inode_path);
                }
                _ => {}
            }
        }

        let (lease_entries, actions) = uploading_files
            .into_values()
            .flatten()
            .map(|b| (LeaseEntry::new(b.locator.clone(), b.length), b))
            .collect::<(Vec<_>, Vec<_>)>();
        let lease = Lease::new_with_blobs(lease_entries);
        UploadPlan { lease, actions }
    }

    /// Updates the local state (self) to be correct in respect to the new
    /// confirmed state (other)
    pub fn update_by_commited(&mut self, other: &Self) {
        let commited_changes = other.performed_changes.len();
        self.performed_changes.drain(..commited_changes);
    }

    pub fn resolve_dir(&self, path: &InodePath) -> Result<&FileTree> {
        self.file_tree.resolve_dir(path.as_slice())
    }

    pub fn resolve_path(&self, path: &InodePath) -> Result<&FileTreeEntry> {
        self.file_tree.resolve_path(path.as_slice())
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
pub struct FileTree(BTreeMap<String, FileTreeEntry>);

impl FileTree {
    fn resolve_dir(&self, path: &[String]) -> Result<&FileTree> {
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
        Ok(cur_dir)
    }

    fn resolve_dir_entry(
        &mut self,
        path: &[String],
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

    fn resolve_path(&self, path: &[String]) -> Result<&FileTreeEntry> {
        let len = path.len();
        if len == 0 {
            return Err(StateError::PathEmpty);
        }

        let tree = self.resolve_dir(&path[..len - 1])?;
        tree.get(&path[len - 1]).ok_or(StateError::BrokenPath)
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum FileTreeEntry {
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
    pub backend_id: BackendId,
    pub uuid: Uuid,
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
    pub fn new_with_blobs(blobs: Vec<LeaseEntry>) -> Self {
        let curr = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs();

        Self {
            valid_until_unix: curr + LEASE_DURATION,
            blobs,
        }
    }

    pub fn new() -> Self {
        Self::new_with_blobs(Vec::new())
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Constructor)]
pub struct LeaseEntry {
    pub locator: BlobLocator,
    pub size: u64,
}

pub struct UploadPlan {
    pub lease: Lease,
    pub actions: Vec<UploadBlob>,
}

#[derive(Clone, Constructor, Debug)]
pub struct UploadBlob {
    pub locator: BlobLocator,
    pub source_file: Uuid,
    pub offset: u64,
    pub length: u64,
}

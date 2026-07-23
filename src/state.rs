use std::{
    collections::{BTreeMap, btree_map},
    time::{SystemTime, UNIX_EPOCH},
};

use derive_more::{Constructor, Display, Error};
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ActiveLeases,
    backend::{BackendKind, BackendStat, BlobId},
    prelude::InodePath,
};

/// 6 hours
pub const LEASE_DURATION: u64 = 6 * 3600;

pub type BackendId = u32;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
pub struct State {
    /// The revision number as visible in the bootstrap
    pub rev: u64,
    pub file_tree: FileTree,
    pub backends: BTreeMap<BackendId, BackendMeta>,
    /// A list of changes that when replayed will transform the previous
    /// revision into the current one
    pub performed_changes: Vec<StateChange>,
}

impl State {
    pub fn new_child(&self) -> Self {
        Self {
            rev: self.rev.checked_add(1).expect("Ran out of revision ids"),
            performed_changes: Vec::new(),
            ..self.clone()
        }
    }

    pub fn perform_change(&mut self, change: StateChange) -> Result<(), StateChangeError> {
        match change.clone() {
            StateChange::AddedBackend(meta) => {
                let new_id = self
                    .backends
                    .last_key_value()
                    .map(|(k, _)| k.checked_add(1).ok_or(StateChangeError::OutOfBackendIds))
                    .transpose()?
                    .unwrap_or_default();
                self.backends.insert(new_id, meta);
            }
            StateChange::RemovedBackend(_) => todo!(),
            StateChange::UploadFile {
                source_file: _,
                mut path,
                file_inode,
            } => {
                // Try find the spot for the file in the filetree
                let Some(filename) = path.pop() else {
                    return Err(StateChangeError::NoFilenameGiven);
                };

                let mut cur_dir = &mut self.file_tree;
                for seg in path.iter() {
                    let next_entry = cur_dir.get_mut(seg).ok_or(StateChangeError::BrokenPath)?;
                    match next_entry {
                        FileTreeEntry::Dir(btree_map) => cur_dir = btree_map,
                        FileTreeEntry::File(_) => return Err(StateChangeError::BrokenPath),
                    }
                }

                let file_entry = match cur_dir.entry(filename) {
                    btree_map::Entry::Vacant(entry) => entry,
                    btree_map::Entry::Occupied(_) => return Err(StateChangeError::FileExists),
                };

                // Try allocate extents for the file
                // TODO

                // All checks passed. Now modifying the state

                file_entry.insert(FileTreeEntry::File(file_inode));
            }
            StateChange::RemoveFile(inode_path) => todo!(),
        }

        self.performed_changes.push(change);

        Ok(())
    }
}

#[derive(Debug, Display, Error)]
pub enum StateChangeError {
    #[display("Ran out of backend ids")]
    OutOfBackendIds,
    #[display("Specified file path contained no filename")]
    NoFilenameGiven,
    #[display("Supplied file path is broken")]
    BrokenPath,
    #[display("File with this name already exists")]
    FileExists,
}

pub type FileTree = BTreeMap<String, FileTreeEntry>;

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
    pub length: u64,
    pub blocks: Vec<Extent>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Extent {
    pub length: u64,
    pub locator: BlobLocator,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobLocator {
    pub backend_id: BackendId,
    pub uuid: Uuid,
}

impl BlobLocator {
    pub fn into_blob_id(self) -> (BackendId, BlobId) {
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

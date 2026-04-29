use std::{
    collections::BTreeMap,
    fmt, io,
    path::{Path, PathBuf},
};

use bimap::BiBTreeMap;
use db::{BackendData, BackendId, Metadata};
use redb::{
    CommitError, Database, DatabaseError, ReadableMultimapTable, ReadableTable, StorageError,
    TableError, TransactionError,
};

mod db;

pub struct App {
    db: Database,
    // Maps backend id to its metadata (ordered by usage) and vice versa
    backends_metadata: RwLock<BiBTreeMap<BackendId, (OrderedFloat<f64>, BackendData)>>,
    // Maps backend id to a loaded Backend trait object
    loaded_backends: RwLock<BTreeMap<BackendId, &'static BackendBox>>,
    next_ids: Mutex<Metadata>,
}

impl App {
    pub async fn load<P: AsRef<Path>>(
        db_path: P,
        mod_dir_path: P,
    ) -> Result<Self, AppLoadingError> {
        let db = Database::create(db_path)?;
        let txn = db.begin_write()?;

        {
            // init tables
            txn.open_table(db::TABLE_BACKEND_TYPES_COUNT)?;
            txn.open_table(db::TABLE_BACKENDS_INIT_DATA)?;
            txn.open_multimap_table(db::TABLE_RELATION_CHILDREN)?;
            txn.open_table(db::TABLE_RELATION_PARENT)?;
        }

        let backends = {
            let mut backends = BTreeMap::new();

            let mut dir = fs::read_dir(mod_dir_path.as_ref())
                .await
                .map_err(|_| AppLoadingError::InvalidModDir(mod_dir_path.as_ref().into()))?;
            while let Some(entry) = dir.next().await {
                let path = entry?.path();
                let library = ParsedBackendMod::load(&path, CTX)
                    .await
                    .map_err(|e| (path, e))?;
                let signature = library.signature;
                backends.insert(signature.as_str(), library);
            }
            backends
        };
        let backend_mods_db_id = {
            let mut backends = BiBTreeMap::new();
            {
                let table = txn.open_table(db::BACKEND_MOD_SIGNATURE)?;
                for backend in table.iter().unwrap() {
                    let backend = backend.unwrap();
                    backends.insert(backend.0.value(), backend.1.value());
                }
            }
            RwLock::new(backends)
        };
        let backends_metadata = {
            let table = txn.open_table(db::TABLE_BACKENDS)?;
            let mut backends_metadata = BiBTreeMap::new();

            for entry in table.iter()? {
                let entry = entry?;
                let (key, val) = (entry.0.value(), entry.1.value());
                let usage = val.free as f64 / val.total as f64;
                backends_metadata.insert(key, (OrderedFloat(usage), val));
            }

            RwLock::new(backends_metadata)
        };
        let next_ids = {
            let backend_type_id = {
                let table = txn.open_table(db::BACKEND_MOD_SIGNATURE)?;
                let id = table.last()?.map(|kv| kv.0.value() + 1).unwrap_or(0);
                id
            };
            let backend_id = {
                let table = txn.open_table(db::TABLE_BACKENDS)?;
                let id = table.last()?.map(|kv| kv.0.value() + 1).unwrap_or(0);
                id
            };
            let inode_id = {
                let table = txn.open_table(db::TABLE_INODES)?;
                let id = table.last()?.map(|kv| kv.0.value() + 1).unwrap_or(0);
                id
            };
            let chunk_id = {
                let table = txn.open_multimap_table(db::TABLE_CHUNKS)?;
                table
                    .iter()?
                    .last()
                    .map(|kv| kv.map(|kv| kv.0.value() + 1))
                    .transpose()?
                    .unwrap_or(0)
            };

            Mutex::new(Metadata {
                backend_type_id,
                backend_id,
                inode_id,
                chunk_id,
            })
        };

        txn.commit()?;

        Ok(App {
            db,
            backend_mods: backends,
            backend_mods_db_id,
            backends_metadata,
            loaded_backends: RwLock::new(BTreeMap::new()),
            next_ids,
        })
    }

    pub async fn list_backend_mods(&self) -> Vec<String> {
        self.backend_mods
            .keys()
            .map(|rstr| rstr.to_string())
            .collect()
    }

    pub async fn add_backend(&self, backend_mod: &str) -> Result<(), AddBackendError> {
        let selected_mod = self
            .backend_mods
            .get(backend_mod)
            .ok_or(AddBackendError::NoSuchBackendMod)?;
        let signature = selected_mod.signature.to_string();
        let init_data = selected_mod
            .add_backend()
            .await
            .map_err(|e| AddBackendError::BackendModError(e.into_string()))
            .into_result()?;

        let db_id_lock = self.backend_mods_db_id.write().await;
        let txn = self.db.begin_write()?;
        let mod_id = match db_id_lock.get_by_right(backend_mod) {
            Some(id) => *id,
            None => {
                let mut next_id_lock = self.next_ids.lock().await;
                let mod_id = next_id_lock.backend_type_id;
                next_id_lock.backend_type_id += 1;
                drop(next_id_lock);

                let mut table = txn.open_table(db::BACKEND_MOD_SIGNATURE)?;
                table.insert(mod_id, &signature)?;

                let mut table = txn.open_table(db::TABLE_BACKEND_TYPES_COUNT)?;
                table.insert(mod_id, 1)?;

                mod_id
            }
        };
        drop(db_id_lock);

        let backend_id = {
            let mut lock = self.next_ids.lock().await;
            let id = lock.backend_id;
            lock.backend_id += 1;
            id
        };
        let backend = selected_mod
            .load_backend(init_data.clone())
            .await
            .into_result()?;
        let storage_usage = backend.get_storage().await.into_result()?;
        let usage = storage_usage.free as f64 / storage_usage.total as f64;
        let backend_data = BackendData {
            free: storage_usage.free,
            total: storage_usage.total,
            chunks_contained: 0,
            mod_id,
        };

        {
            let mut table = txn.open_table(db::TABLE_BACKENDS)?;
            table.insert(backend_id, backend_data)?;

            let mut table = txn.open_table(db::TABLE_BACKENDS_INIT_DATA)?;
            table.insert(backend_id, init_data.as_ref_())?;
        }

        self.backend_mods_db_id
            .write()
            .await
            .insert(backend_id, signature);
        let metadata = (OrderedFloat(usage), backend_data);
        self.backends_metadata
            .write()
            .await
            .insert(backend_id, metadata);
        self.loaded_backends
            .write()
            .await
            .insert(backend_id, leak_value(backend));

        txn.commit()?;
        Ok(())
    }

    pub async fn get_backend(&self, id: BackendId) -> Result<&'static BackendBox, GetBackendError> {
        let read_lock = self.loaded_backends.read().await;
        let val = read_lock.get(&id).copied();
        drop(read_lock);
        match val {
            Some(v) => Ok(v),
            None => {
                let txn = self.db.begin_read()?;
                let table = txn.open_table(db::TABLE_BACKENDS)?;
                let backend_data = table.get(id)?.ok_or(GetBackendError::NotFound)?.value();
                drop(table);
                let table = txn.open_table(db::TABLE_BACKENDS_INIT_DATA)?;
                let init_data =
                    RVec::from(table.get(id)?.ok_or(GetBackendError::NotFound)?.value());
                drop(table);
                drop(txn);

                let backend_mod_signature = self
                    .backend_mods_db_id
                    .read()
                    .await
                    .get_by_left(&backend_data.mod_id)
                    .unwrap()
                    .to_string();
                let backend_mod = self
                    .backend_mods
                    .get(backend_mod_signature.as_str())
                    .ok_or(GetBackendError::BackendModNotFound(backend_mod_signature))?;

                let loaded_backend = backend_mod.load_backend(init_data).await.into_result()?;
                let backend_ref = leak_value(loaded_backend);
                self.loaded_backends.write().await.insert(id, backend_ref);

                Ok(backend_ref)
            }
        }
    }

    #[cfg(debug_assertions)]
    pub fn show_all_tables(&self) {
        let txn = self.db.begin_read().unwrap();

        println!("TABLE_BACKEND_TYPES_SIGNATURE");
        let table = txn.open_table(db::BACKEND_MOD_SIGNATURE).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!("{}: {}", row.0.value(), row.1.value());
        }
        println!();

        println!("TABLE_BACKEND_TYPES_COUNT");
        let table = txn.open_table(db::TABLE_BACKEND_TYPES_COUNT).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!("{}: {}", row.0.value(), row.1.value());
        }
        println!();

        println!("TABLE_BACKENDS");
        let table = txn.open_table(db::TABLE_BACKENDS).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!("{}: {:?}", row.0.value(), row.1.value());
        }
        println!();

        println!("TABLE_BACKENDS_INIT_DATA");
        let table = txn.open_table(db::TABLE_BACKENDS_INIT_DATA).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!("{}: {:?}", row.0.value(), row.1.value());
        }
        println!();

        println!("TABLE_CHUNKS");
        let table = txn.open_multimap_table(db::TABLE_CHUNKS).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!(
                "{}: {:?}",
                row.0.value(),
                row.1.map(|v| v.unwrap().value()).collect::<Vec<_>>()
            );
        }
        println!();

        println!("TABLE_INODES");
        let table = txn.open_table(db::TABLE_INODES).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!("{}: {:?}", row.0.value(), row.1.value());
        }
        println!();

        println!("TABLE_RELATION_CHILDREN");
        let table = txn
            .open_multimap_table(db::TABLE_RELATION_CHILDREN)
            .unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!(
                "{}: {:?}",
                row.0.value(),
                row.1.map(|v| v.unwrap().value()).collect::<Vec<_>>()
            );
        }
        println!();

        println!("TABLE_RELATION_PARENT");
        let table = txn.open_table(db::TABLE_RELATION_PARENT).unwrap();
        for row in table.iter().unwrap() {
            let row = row.unwrap();
            println!("{}: {}", row.0.value(), row.1.value());
        }
        println!();
    }
}

impl fmt::Debug for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let binding = self.loaded_backends.read_blocking();
        let loaded_backends = binding
            .iter()
            .map(|v| (v.0, v.1.get_signature()))
            .collect::<BTreeMap<_, _>>();

        f.debug_struct("App")
            .field("backend_mods", &self.backend_mods)
            .field("backend_mods_db_id", &self.backend_mods_db_id)
            .field("backends_metadata", &self.backends_metadata)
            .field("loaded_backends", &loaded_backends)
            .field("next_ids", &self.next_ids)
            .finish()
    }
}

#[derive(Debug)]
pub enum AppLoadingError {
    Io(io::Error),
    InvalidModDir(PathBuf),
    Db(DatabaseError),
    DbTransaction(TransactionError),
    DbTable(TableError),
    DbCommit(CommitError),
    DbStorage(StorageError),
    Lib(PathBuf, LibraryError),
}

impl From<io::Error> for AppLoadingError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<DatabaseError> for AppLoadingError {
    fn from(value: DatabaseError) -> Self {
        Self::Db(value)
    }
}

impl From<TransactionError> for AppLoadingError {
    fn from(value: TransactionError) -> Self {
        Self::DbTransaction(value)
    }
}

impl From<TableError> for AppLoadingError {
    fn from(value: TableError) -> Self {
        Self::DbTable(value)
    }
}

impl From<CommitError> for AppLoadingError {
    fn from(value: CommitError) -> Self {
        Self::DbCommit(value)
    }
}

impl From<StorageError> for AppLoadingError {
    fn from(value: StorageError) -> Self {
        Self::DbStorage(value)
    }
}

impl From<(PathBuf, LibraryError)> for AppLoadingError {
    fn from(value: (PathBuf, LibraryError)) -> Self {
        Self::Lib(value.0, value.1)
    }
}

impl std::error::Error for AppLoadingError {}
impl fmt::Display for AppLoadingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppLoadingError::Io(error) => write!(f, "IO error: {error}"),
            AppLoadingError::InvalidModDir(path_buf) => {
                write!(f, "\"{}\" is not a directory", path_buf.display())
            }
            AppLoadingError::Db(database_error) => {
                write!(f, "Failed loading the database: {database_error}")
            }
            AppLoadingError::DbTransaction(transaction_error) => {
                write!(f, "Failed loading the database: {transaction_error}")
            }
            AppLoadingError::DbTable(table_error) => {
                write!(f, "Failed loading the database: {table_error}")
            }
            AppLoadingError::DbCommit(commit_error) => {
                write!(f, "Failed loading the database: {commit_error}")
            }
            AppLoadingError::DbStorage(storage_error) => {
                write!(f, "Failed loading the database: {storage_error}")
            }
            AppLoadingError::Lib(path, library_error) => {
                write!(f, "Failed loading a module at {}: ", path.display())?;
                match library_error {
                    LibraryError::OpenError { err, .. } => {
                        write!(f, "Could not open the library because: {err}",)
                    }
                    LibraryError::GetSymbolError { symbol, err, .. } => write!(
                        f,
                        "Could not load symbol: \"{}\" because: {}",
                        String::from_utf8_lossy(symbol),
                        err,
                    ),
                    LibraryError::ParseVersionError(x) => fmt::Display::fmt(x, f),
                    LibraryError::IncompatibleVersionNumber {
                        library_name,
                        expected_version,
                        actual_version,
                    } => write!(
                        f,
                        "\"{}\" library version mismatch: user: {}, library: {}",
                        library_name, expected_version, actual_version,
                    ),
                    LibraryError::RootModule { err, version, .. } => {
                        write!(
                            f,
                            "An error ocurred while loading this module: v{}: {}",
                            version, err
                        )
                    }
                    LibraryError::AbiInstability(x) => fmt::Display::fmt(x, f),
                    LibraryError::InvalidAbiHeader(_) => write!(f, "Mod abi header mismatch",),
                    LibraryError::InvalidCAbi { expected, found } => {
                        write! {f,
                            "The C abi of the module is different than expected. Found: {}, Expected: {}",
                            found,
                            expected,
                        }
                    }
                    LibraryError::Many(list) => {
                        for e in list {
                            fmt::Display::fmt(e, f)?;
                        }
                        Ok(())
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum AddBackendError {
    NoSuchBackendMod,
    BackendModError(String),
    NewBackendError(NewBackendError),
    BackendError(ReadError),
    DbStorage(StorageError),
    DbTable(TableError),
}

impl From<TransactionError> for AddBackendError {
    fn from(value: TransactionError) -> Self {
        match value {
            TransactionError::Storage(err) => Self::DbStorage(err),
            TransactionError::ReadTransactionStillInUse(..) => unimplemented!(),
            _ => todo!(),
        }
    }
}

impl From<NewBackendError> for AddBackendError {
    fn from(value: NewBackendError) -> Self {
        Self::NewBackendError(value)
    }
}

impl From<ReadError> for AddBackendError {
    fn from(value: ReadError) -> Self {
        Self::BackendError(value)
    }
}

impl From<StorageError> for AddBackendError {
    fn from(value: StorageError) -> Self {
        Self::DbStorage(value)
    }
}

impl From<CommitError> for AddBackendError {
    fn from(value: CommitError) -> Self {
        match value {
            CommitError::Storage(err) => err.into(),
            _ => todo!(),
        }
    }
}

impl From<TableError> for AddBackendError {
    fn from(value: TableError) -> Self {
        match value {
            TableError::Storage(err) => err.into(),
            err => AddBackendError::DbTable(err),
        }
    }
}

#[derive(Debug)]
pub enum GetBackendError {
    /// No backend with the given id in the database
    NotFound,
    InitFailed(BackendInitError),
    BackendModNotFound(String),
    NewBackendFailed(NewBackendError),
}

impl From<StorageError> for GetBackendError {
    fn from(value: StorageError) -> Self {
        Self::InitFailed(value.into())
    }
}

impl From<TransactionError> for GetBackendError {
    fn from(value: TransactionError) -> Self {
        Self::InitFailed(value.into())
    }
}

impl From<TableError> for GetBackendError {
    fn from(value: TableError) -> Self {
        Self::InitFailed(value.into())
    }
}

impl From<NewBackendError> for GetBackendError {
    fn from(value: NewBackendError) -> Self {
        Self::NewBackendFailed(value)
    }
}

#[derive(Debug)]
pub enum BackendInitError {
    Storage(StorageError),
    Table(TableError),
}

impl From<StorageError> for BackendInitError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<TransactionError> for BackendInitError {
    fn from(value: TransactionError) -> Self {
        match value {
            TransactionError::Storage(err) => Self::Storage(err),
            _ => todo!(),
        }
    }
}

impl From<TableError> for BackendInitError {
    fn from(value: TableError) -> Self {
        match value {
            TableError::Storage(err) => err.into(),
            err => Self::Table(err),
        }
    }
}

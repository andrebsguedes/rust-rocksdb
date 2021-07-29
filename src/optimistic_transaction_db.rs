use crate::{
//     column_family::AsColumnFamilyRef,
//     column_family::BoundColumnFamily,
//     column_family::UnboundColumnFamily,
    db::{ThreadMode, DBWithThreadMode, SingleThreaded},
    transaction::Transaction,
//     db_options::OptionsMustOutliveDB,
    ffi,
    ffi_util::{/*from_cstr, opt_bytes_to_ptr, raw_data, */to_cpath},
    ColumnFamilyDescriptor, Options, DEFAULT_COLUMN_FAMILY_NAME, Error,
//     CompactOptions, DBIteratorWithThreadMode,
//     DBPinnableSlice, DBRawIteratorWithThreadMode, DBWALIterator, Direction, FlushOptions,
//     IngestExternalFileOptions, IteratorMode, SnapshotWithThreadMode,
//     WriteBatch, ReadOptions
    WriteOptions
};

#[cfg(feature = "multi-threaded-cf")]
use crate::db::MultiThreaded;

//
use libc::{self, c_char, c_int, c_uchar/*, c_void, size_t*/};
use std::collections::BTreeMap;
use std::ffi::{/*CStr, */CString};
use std::fmt;
use std::fs;
use std::iter;
use std::path::Path;
// use std::path::PathBuf;
use std::ptr;
use std::mem::ManuallyDrop;
// use std::slice;
// use std::str;
// use std::sync::Arc;
// use std::sync::RwLock;
// use std::time::Duration;

pub struct OptimisticTransactionOptions {
    inner: *mut ffi::rocksdb_optimistictransaction_options_t,
}

impl OptimisticTransactionOptions {
    /// Create new optimistic transaction options
    pub fn new() -> OptimisticTransactionOptions {
        unsafe {
            let inner = ffi::rocksdb_optimistictransaction_options_create();
            OptimisticTransactionOptions { inner }
        }
    }

    /// Set a snapshot at start of transaction by setting set_snapshot=true
    /// Default: false
    pub fn set_snapshot(&mut self, set_snapshot: bool) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_set_set_snapshot(
                self.inner,
                set_snapshot as c_uchar,
            );
        }
    }
}

impl Drop for OptimisticTransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_destroy(self.inner);
        }
    }
}

impl Default for OptimisticTransactionOptions {
    fn default() -> OptimisticTransactionOptions {
        OptimisticTransactionOptions::new()
    }
}

/// A RocksDB optimistic transaction database.
///
/// See crate level documentation for a simple usage example.
pub struct OptimisticTransactionDBWithThreadMode<T: ThreadMode> {
    pub(crate) inner: *mut ffi::rocksdb_optimistictransactiondb_t,
    base_db: ManuallyDrop<DBWithThreadMode<T>>
}

#[cfg(not(feature = "multi-threaded-cf"))]
pub type OptimisticTransactionDB = OptimisticTransactionDBWithThreadMode<SingleThreaded>;

#[cfg(feature = "multi-threaded-cf")]
pub type OptimisticTransactionDB = OptimisticTransactionDBWithThreadMode<MultiThreaded>;

// Safety note: auto-implementing Send on most db-related types is prevented by the inner FFI
// pointer. In most cases, however, this pointer is Send-safe because it is never aliased and
// rocksdb internally does not rely on thread-local information for its user-exposed types.
unsafe impl<T: ThreadMode> Send for OptimisticTransactionDBWithThreadMode<T> {}

// Sync is similarly safe for many types because they do not expose interior mutability, and their
// use within the rocksdb library is generally behind a const reference
unsafe impl<T: ThreadMode> Sync for OptimisticTransactionDBWithThreadMode<T> {}

impl <T: ThreadMode> OptimisticTransactionDBWithThreadMode<T> {
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Self::open(&opts, path)
    }

    /// Opens the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        Self::open_cf(opts, path, None::<&str>)
    }

    /// Opens a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        Self::open_cf_descriptors(opts, path, cfs)
    }

    /// Internal implementation for opening RocksDB.
    fn open_cf_descriptors<P, I>(
        opts: &Options,
        path: P,
        cfs: I,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let cfs: Vec<_> = cfs.into_iter().collect();
        let outlive = iter::once(opts.outlive.clone())
            .chain(cfs.iter().map(|cf| cf.options.outlive.clone()))
            .collect();

        let cpath = to_cpath(&path)?;

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_optimistictransactiondb_t;
        let mut cf_map = BTreeMap::new();

        if cfs.is_empty() {
            db = Self::open_raw(opts, &cpath)?;
        } else {
            let mut cfs_v = cfs;
            // Always open the default column family.
            if !cfs_v.iter().any(|cf| cf.name == DEFAULT_COLUMN_FAMILY_NAME) {
                cfs_v.push(ColumnFamilyDescriptor {
                    name: String::from(DEFAULT_COLUMN_FAMILY_NAME),
                    options: Options::default(),
                });
            }
            // We need to store our CStrings in an intermediate vector
            // so that their pointers remain valid.
            let c_cfs: Vec<CString> = cfs_v
                .iter()
                .map(|cf| CString::new(cf.name.as_bytes()).unwrap())
                .collect();

            let cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

            // These handles will be populated by DB.
            let mut cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

            let cfopts: Vec<_> = cfs_v
                .iter()
                .map(|cf| cf.options.inner as *const _)
                .collect();

            db = Self::open_cf_raw(
                opts,
                &cpath,
                &cfs_v,
                &cfnames,
                &cfopts,
                &mut cfhandles,
            )?;
            for handle in &cfhandles {
                if handle.is_null() {
                    return Err(Error::new(
                        "Received null column family handle from DB.".to_owned(),
                    ));
                }
            }

            for (cf_desc, inner) in cfs_v.iter().zip(cfhandles) {
                cf_map.insert(cf_desc.name.clone(), inner);
            }
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        let base_db_ptr = unsafe { ffi::rocksdb_optimistictransactiondb_get_base_db(db) };

        Ok(Self {
            inner: db,
            base_db: ManuallyDrop::new(DBWithThreadMode::<T>::from_optimistic_txn_db_base(
                base_db_ptr,
                T::new_cf_map_internal(cf_map),
                path.as_ref().to_path_buf(),
                outlive,
            ))
        })
    }

    fn open_raw(
        opts: &Options,
        cpath: &CString,
    ) -> Result<*mut ffi::rocksdb_optimistictransactiondb_t, Error> {
        let db = unsafe {
            ffi_try!(ffi::rocksdb_optimistictransactiondb_open(
                opts.inner,
                cpath.as_ptr(),
            ))
        };
        Ok(db)
    }

    fn open_cf_raw(
        opts: &Options,
        cpath: &CString,
        cfs_v: &[ColumnFamilyDescriptor],
        cfnames: &[*const c_char],
        cfopts: &[*const ffi::rocksdb_options_t],
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>
    ) -> Result<*mut ffi::rocksdb_optimistictransactiondb_t, Error> {
        let db = unsafe {
            ffi_try!(ffi::rocksdb_optimistictransactiondb_open_column_families(
                opts.inner,
                cpath.as_ptr(),
                cfs_v.len() as c_int,
                cfnames.as_ptr(),
                cfopts.as_ptr(),
                cfhandles.as_mut_ptr(),
            ))
        };
        Ok(db)
    }

    pub fn transaction_opt(
        &self,
        write_opts: &WriteOptions,
        txn_opts: &OptimisticTransactionOptions
    ) -> Transaction<Self> {
        unsafe {
            let inner = ffi::rocksdb_optimistictransaction_begin(
                self.inner,
                write_opts.inner,
                txn_opts.inner,
                ptr::null_mut()
            );
            Transaction::new(inner)
        }
    }

    pub fn transaction(&self) -> Transaction<Self> {
        let write_opts = WriteOptions::default();
        let optimistic_txn_opts = OptimisticTransactionOptions::default();
        self.transaction_opt(&write_opts, &optimistic_txn_opts)
    }
}

impl<T: ThreadMode> Drop for OptimisticTransactionDBWithThreadMode<T> {
    fn drop(&mut self) {
        unsafe {
            // close for base_db will be run on its destructor
            ManuallyDrop::drop(&mut self.base_db);
            ffi::rocksdb_optimistictransactiondb_close(self.inner);
        }
    }
}

impl<T: ThreadMode> fmt::Debug for OptimisticTransactionDBWithThreadMode<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OptimisticTransactionDB {{ path: {:?} }}", self.base_db.path())
    }
}

impl<T: ThreadMode> std::ops::Deref for OptimisticTransactionDBWithThreadMode<T> {
    type Target = DBWithThreadMode<T>;

    fn deref(&self) -> &Self::Target {
        &self.base_db
    }
}

impl<T: ThreadMode> std::ops::DerefMut for OptimisticTransactionDBWithThreadMode<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base_db
    }
}

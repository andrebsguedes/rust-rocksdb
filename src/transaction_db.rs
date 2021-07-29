use crate::{
    column_family::AsColumnFamilyRef,
    column_family::BoundColumnFamily,
    column_family::UnboundColumnFamily,
    db::{ThreadMode, DBWithThreadMode, SingleThreaded, MultiThreaded},
    transaction::Transaction,
//     db_options::OptionsMustOutliveDB,
    ffi,
    ffi_util::{/*from_cstr, opt_bytes_to_ptr, raw_data, */to_cpath},
    ColumnFamily, ColumnFamilyDescriptor, Options, DEFAULT_COLUMN_FAMILY_NAME, Error,
//     CompactOptions, DBIteratorWithThreadMode,
//     DBPinnableSlice, DBRawIteratorWithThreadMode, DBWALIterator, Direction, FlushOptions,
//     IngestExternalFileOptions, IteratorMode, SnapshotWithThreadMode,
    WriteBatch, WriteOptions, ReadOptions
};
//
use libc::{self, c_char, c_int, c_uchar, c_void, size_t};
use std::collections::BTreeMap;
use std::ffi::{/*CStr, */CString};
use std::fmt;
use std::fs;
// use std::iter;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
// use std::slice;
// use std::str;
use std::sync::Arc;
// use std::sync::RwLock;
// use std::time::Duration;

pub struct TransactionDBOptions {
    inner: *mut ffi::rocksdb_transactiondb_options_t,
}

impl TransactionDBOptions {
    /// Create new transaction options
    pub fn new() -> TransactionDBOptions {
        unsafe {
            let inner = ffi::rocksdb_transactiondb_options_create();
            TransactionDBOptions { inner }
        }
    }

    pub fn set_default_lock_timeout(&self, default_lock_timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_default_lock_timeout(
                self.inner,
                default_lock_timeout,
            )
        }
    }

    pub fn set_max_num_locks(&self, max_num_locks: i64) {
        unsafe { ffi::rocksdb_transactiondb_options_set_max_num_locks(self.inner, max_num_locks) }
    }

    pub fn set_num_stripes(&self, num_stripes: usize) {
        unsafe { ffi::rocksdb_transactiondb_options_set_num_stripes(self.inner, num_stripes) }
    }

    pub fn set_transaction_lock_timeout(&self, txn_lock_timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_transaction_lock_timeout(
                self.inner,
                txn_lock_timeout,
            )
        }
    }
}

impl Drop for TransactionDBOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_options_destroy(self.inner);
        }
    }
}

impl Default for TransactionDBOptions {
    fn default() -> TransactionDBOptions {
        TransactionDBOptions::new()
    }
}

pub struct TransactionOptions {
    inner: *mut ffi::rocksdb_transaction_options_t,
}

impl TransactionOptions {
    /// Create new transaction options
    pub fn new() -> TransactionOptions {
        unsafe {
            let inner = ffi::rocksdb_transaction_options_create();
            TransactionOptions { inner }
        }
    }

    pub fn set_deadlock_detect(&self, deadlock_detect: bool) {
        unsafe {
            ffi::rocksdb_transaction_options_set_deadlock_detect(
                self.inner,
                deadlock_detect as c_uchar,
            )
        }
    }

    pub fn set_deadlock_detect_depth(&self, depth: i64) {
        unsafe { ffi::rocksdb_transaction_options_set_deadlock_detect_depth(self.inner, depth) }
    }

    pub fn set_expiration(&self, expiration: i64) {
        unsafe { ffi::rocksdb_transaction_options_set_expiration(self.inner, expiration) }
    }

    pub fn set_lock_timeout(&self, lock_timeout: i64) {
        unsafe { ffi::rocksdb_transaction_options_set_lock_timeout(self.inner, lock_timeout) }
    }

    pub fn set_max_write_batch_size(&self, size: usize) {
        unsafe { ffi::rocksdb_transaction_options_set_max_write_batch_size(self.inner, size) }
    }

    pub fn set_snapshot(&mut self, set_snapshot: bool) {
        unsafe {
            ffi::rocksdb_transaction_options_set_set_snapshot(self.inner, set_snapshot as c_uchar);
        }
    }
}

impl Drop for TransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_options_destroy(self.inner);
        }
    }
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        TransactionOptions::new()
    }
}

/// A RocksDB transaction database.
///
/// See crate level documentation for a simple usage example.
pub struct TransactionDBWithThreadMode<T: ThreadMode> {
    pub(crate) inner: *mut ffi::rocksdb_transactiondb_t,
    cfs: T, // Column families are held differently depending on thread mode
    path: PathBuf,
}

impl <T: ThreadMode> TransactionDBWithThreadMode<T> {
    /// Opens the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, txn_db_opts: &TransactionDBOptions, path: P) -> Result<Self, Error> {
        Self::open_cf(opts, txn_db_opts, path, None::<&str>)
    }

    /// Opens a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P, I, N>(opts: &Options, txn_db_opts: &TransactionDBOptions, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        Self::open_cf_descriptors(opts, txn_db_opts, path, cfs)
    }

    /// Internal implementation for opening RocksDB.
    fn open_cf_descriptors<P, I>(
        opts: &Options,
        txn_db_opts: &TransactionDBOptions,
        path: P,
        cfs: I,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let cfs: Vec<_> = cfs.into_iter().collect();
//         let outlive = iter::once(opts.outlive.clone())
//             .chain(cfs.iter().map(|cf| cf.options.outlive.clone()))
//             .collect();

        let cpath = to_cpath(&path)?;

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_transactiondb_t;
        let mut cf_map = BTreeMap::new();

        if cfs.is_empty() {
            db = Self::open_raw(opts, txn_db_opts, &cpath)?;
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
                txn_db_opts,
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

        Ok(Self {
            inner: db,
            path: path.as_ref().to_path_buf(),
            cfs: T::new_cf_map_internal(cf_map),
            // _outlive: outlive,
        })
    }

    fn open_raw(
        opts: &Options,
        txn_db_opts: &TransactionDBOptions,
        cpath: &CString,
    ) -> Result<*mut ffi::rocksdb_transactiondb_t, Error> {
        let db = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open(
                opts.inner,
                txn_db_opts.inner,
                cpath.as_ptr(),
            ))
        };
        Ok(db)
    }

    fn open_cf_raw(
        opts: &Options,
        txn_db_opts: &TransactionDBOptions,
        cpath: &CString,
        cfs_v: &[ColumnFamilyDescriptor],
        cfnames: &[*const c_char],
        cfopts: &[*const ffi::rocksdb_options_t],
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>
    ) -> Result<*mut ffi::rocksdb_transactiondb_t, Error> {
        let db = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open_column_families(
                opts.inner,
                txn_db_opts.inner,
                cpath.as_ptr(),
                cfs_v.len() as c_int,
                cfnames.as_ptr(),
                cfopts.as_ptr(),
                cfhandles.as_mut_ptr(),
            ))
        };
        Ok(db)
    }

    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        DBWithThreadMode::<T>::list_cf(opts, path)
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        DBWithThreadMode::<T>::destroy(opts, path)
    }

    pub fn repair<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        DBWithThreadMode::<T>::repair(opts, path)
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    pub fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_write(self.inner, writeopts.inner, batch.inner));
        }
        Ok(())
    }

    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        self.write_opt(batch, &WriteOptions::default())
    }

    pub fn write_without_wal(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        self.write_opt(batch, &wo)
    }

    pub fn get_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. This is a fairly trivial call, and its \
                 failure may be indicative of a mis-compiled or mis-loaded RocksDB library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let mut val_len: size_t = 0;

            let val = ffi_try!(ffi::rocksdb_transactiondb_get(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            ));
            if val.is_null() {
                Ok(None)
            } else {
                let value = crate::ffi_util::raw_data(val, val_len);
                ffi::rocksdb_free(val as *mut c_void);
                Ok(value)
            }
        }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        self.get_opt(key.as_ref(), &ReadOptions::default())
    }

    pub fn get_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. This is a fairly trivial call, and its \
                 failure may be indicative of a mis-compiled or mis-loaded RocksDB library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let mut val_len: size_t = 0;

            let val = ffi_try!(ffi::rocksdb_transactiondb_get_cf(
                self.inner,
                readopts.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            ));
            if val.is_null() {
                Ok(None)
            } else {
                let value = crate::ffi_util::raw_data(val, val_len);
                ffi::rocksdb_free(val as *mut c_void);
                Ok(value)
            }
        }
    }

    pub fn get_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.get_cf_opt(cf, key.as_ref(), &ReadOptions::default())
    }

    fn create_inner_cf_handle(
        &self,
        name: &str,
        opts: &Options,
    ) -> Result<*mut ffi::rocksdb_column_family_handle_t, Error> {
        let cf_name = if let Ok(c) = CString::new(name.as_bytes()) {
            c
        } else {
            return Err(Error::new(
                "Failed to convert path to CString when creating cf".to_owned(),
            ));
        };
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_create_column_family(
                self.inner,
                opts.inner,
                cf_name.as_ptr(),
            ))
        })
    }

    pub fn put_opt<K, V>(&self, key: K, value: V, writeopts: &WriteOptions) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_opt(key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn put_cf_opt<K, V>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        value: V,
        writeopts: &WriteOptions,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put_cf(
                self.inner,
                writeopts.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put_cf<K, V>(&self, cf: &impl AsColumnFamilyRef, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_cf_opt(cf, key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn merge_opt<K, V>(&self, key: K, value: V, writeopts: &WriteOptions) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_merge(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_opt(key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn merge_cf_opt<K, V>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        value: V,
        writeopts: &WriteOptions,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_merge_cf(
                self.inner,
                writeopts.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge_cf<K, V>(&self, cf: &impl AsColumnFamilyRef, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_cf_opt(cf, key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn delete_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        let key = key.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_delete(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.delete_opt(key.as_ref(), &WriteOptions::default())
    }

    pub fn delete_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        let key = key.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_delete_cf(
                self.inner,
                writeopts.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
    ) -> Result<(), Error> {
        self.delete_cf_opt(cf, key.as_ref(), &WriteOptions::default())
    }

    pub fn transaction(
        &self,
        write_opts: &WriteOptions,
        txn_opts: &TransactionOptions
    ) -> Transaction<Self> {
        unsafe {
            let inner = ffi::rocksdb_transaction_begin(
                self.inner,
                write_opts.inner,
                txn_opts.inner,
                ptr::null_mut()
            );
            Transaction::new(inner)
        }
    }
}

impl TransactionDBWithThreadMode<SingleThreaded> {
    /// Creates column family with given name and options
    pub fn create_cf<N: AsRef<str>>(&mut self, name: N, opts: &Options) -> Result<(), Error> {
        let inner = self.create_inner_cf_handle(name.as_ref(), opts)?;
        self.cfs
            .cfs
            .insert(name.as_ref().to_string(), ColumnFamily { inner });
        Ok(())
    }

    /// Drops the column family with the given name
    pub fn drop_cf(&mut self, _name: &str) -> Result<(), Error> {
        unimplemented!("drop_column_family not exposed on C API for transactiondb");
    }

    /// Returns the underlying column family handle
    pub fn cf_handle(&self, name: &str) -> Option<&ColumnFamily> {
        self.cfs.cfs.get(name)
    }
}

impl TransactionDBWithThreadMode<MultiThreaded> {
    /// Creates column family with given name and options
    pub fn create_cf<N: AsRef<str>>(&self, name: N, opts: &Options) -> Result<(), Error> {
        let inner = self.create_inner_cf_handle(name.as_ref(), opts)?;
        self.cfs.cfs.write().unwrap().insert(
            name.as_ref().to_string(),
            Arc::new(UnboundColumnFamily { inner }),
        );
        Ok(())
    }

    /// Drops the column family with the given name by internally locking the inner column
    /// family map. This avoids needing `&mut self` reference
    pub fn drop_cf(&self, _name: &str) -> Result<(), Error> {
        unimplemented!("drop_column_family not exposed on C API for transactiondb");
    }

    /// Returns the underlying column family handle
    pub fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily>> {
        self.cfs
            .cfs
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .map(UnboundColumnFamily::bound_column_family)
    }
}

impl<T: ThreadMode> Drop for TransactionDBWithThreadMode<T> {
    fn drop(&mut self) {
        unsafe {
            self.cfs.drop_all_cfs_internal();
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}

impl<T: ThreadMode> fmt::Debug for TransactionDBWithThreadMode<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TransactionDB {{ path: {:?} }}", self.path())
    }
}

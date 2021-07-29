use crate::{
    Error, ReadOptions, AsColumnFamilyRef
};
use libc::{c_char, c_uchar, c_void, size_t};
use std::marker::PhantomData;
use librocksdb_sys as ffi;

pub struct Transaction<'a, T> {
    inner: *mut ffi::rocksdb_transaction_t,
    db: PhantomData<&'a T>,
}

impl<'a, T> Transaction<'a, T> {
    pub(crate) fn new(inner: *mut ffi::rocksdb_transaction_t) -> Transaction<'a, T> {
        Transaction {
            inner,
            db: PhantomData,
        }
    }

    /// commits a transaction
    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(self.inner));
        }
        Ok(())
    }

    /// Transaction rollback
    pub fn rollback(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback(self.inner)) }
        Ok(())
    }

    /// Transaction rollback to savepoint
    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        unsafe { ffi_try!(ffi::rocksdb_transaction_rollback_to_savepoint(self.inner)) }
        Ok(())
    }

    /// Set savepoint for transaction
    pub fn set_savepoint(&self) {
        unsafe { ffi::rocksdb_transaction_set_savepoint(self.inner) }
    }

//     /// Get Snapshot
//     pub fn snapshot(&'a self) -> TransactionSnapshot<'a, T> {
//         unsafe {
//             let snapshot = ffi::rocksdb_transaction_get_snapshot(self.inner);
//             TransactionSnapshot {
//                 inner: snapshot,
//                 db: self,
//             }
//         }
//     }

    /// Get For Update
    /// ReadOptions: Default
    /// exclusive: true
    pub fn get_for_update<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        let opt = ReadOptions::default();
        self.get_for_update_opt(key, &opt, true)
    }

    /// Get For Update with custom ReadOptions and exclusive
    pub fn get_for_update_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update(
                self.inner,
                readopts.inner,
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
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

    pub fn get_for_update_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<Vec<u8>>, Error> {
        let opt = ReadOptions::default();
        self.get_for_update_cf_opt(cf, key, &opt, true)
    }

    pub fn get_for_update_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
        exclusive: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key = key.as_ref();
        let key_ptr = key.as_ptr() as *const c_char;
        let key_len = key.len() as size_t;
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update_cf(
                self.inner,
                readopts.inner,
                cf.inner(),
                key_ptr,
                key_len,
                &mut val_len,
                exclusive as c_uchar,
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

            let val = ffi_try!(ffi::rocksdb_transaction_get(
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

            let val = ffi_try!(ffi::rocksdb_transaction_get_cf(
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

    pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put(
                self.inner,
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
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put_cf(
                self.inner,
                cf.inner(),
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
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_merge(
                self.inner,
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
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_merge_cf(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_cf<K>(&self, cf: &impl AsColumnFamilyRef, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete_cf(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }
}

impl<'a, T> Drop for Transaction<'a, T> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

// impl<'a, T> Iterate for Transaction<'a, T> {
//     fn get_raw_iter(&self, readopts: &ReadOptions) -> DBRawIterator {
//         unsafe {
//             DBRawIterator {
//                 inner: ffi::rocksdb_transaction_create_iterator(self.inner, readopts.handle()),
//                 db: PhantomData,
//             }
//         }
//     }
// }
//
// impl<'a, T> IterateCF for Transaction<'a, T> {
//     fn get_raw_iter_cf(
//         &self,
//         cf_handle: &ColumnFamily,
//         readopts: &ReadOptions,
//     ) -> Result<DBRawIterator, Error> {
//         unsafe {
//             Ok(DBRawIterator {
//                 inner: ffi::rocksdb_transaction_create_iterator_cf(
//                     self.inner,
//                     readopts.handle(),
//                     cf_handle.inner,
//                 ),
//                 db: PhantomData,
//             })
//         }
//     }
// }

pub struct TransactionSnapshot<'a, T> {
    db: &'a Transaction<'a, T>,
    inner: *const ffi::rocksdb_snapshot_t,
}

// impl<'a, T> GetCF<ReadOptions> for TransactionSnapshot<'a, T>
// where
//     Transaction<'a, T>: GetCF<ReadOptions>,
// {
//     fn get_cf_full<K: AsRef<[u8]>>(
//         &self,
//         cf: Option<&ColumnFamily>,
//         key: K,
//         readopts: Option<&ReadOptions>,
//     ) -> Result<Option<DBVector>, Error> {
//         let mut ro = readopts.cloned().unwrap_or_default();
//         ro.set_snapshot(self);
//         self.db.get_cf_full(cf, key, Some(&ro))
//     }
// }

impl<'a, T> Drop for TransactionSnapshot<'a, T> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_free(self.inner as *mut c_void);
        }
    }
}

// impl<'a, T: Iterate> Iterate for TransactionSnapshot<'a, T> {
//     fn get_raw_iter(&self, readopts: &ReadOptions) -> DBRawIterator {
//         let mut readopts = readopts.to_owned();
//         readopts.set_snapshot(self);
//         self.db.get_raw_iter(&readopts)
//     }
// }
//
// impl<'a, T: IterateCF> IterateCF for TransactionSnapshot<'a, T> {
//     fn get_raw_iter_cf(
//         &self,
//         cf_handle: &ColumnFamily,
//         readopts: &ReadOptions,
//     ) -> Result<DBRawIterator, Error> {
//         let mut readopts = readopts.to_owned();
//         readopts.set_snapshot(self);
//         self.db.get_raw_iter_cf(cf_handle, &readopts)
//     }
// }


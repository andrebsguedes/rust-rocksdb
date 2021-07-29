mod util;

use std::{mem, sync::Arc, thread, time::Duration, convert::TryInto};

use pretty_assertions::assert_eq;

use rocksdb::{
    perf::get_memory_usage_stats, OptimisticTransactionDBWithThreadMode, Error,
    IteratorMode, MultiThreaded, ReadOptions, Options,
    SingleThreaded, WriteBatch, OptimisticTransactionDB,
};

use util::DBPath;

#[test]
fn external() {
    let path = DBPath::new("_rust_optimistic_transaction_db_externaltest");

    {
        let db = OptimisticTransactionDB::open_default(&path).unwrap();

        assert!(db.put(b"k1", b"v1111").is_ok());

        let r: Result<Option<Vec<u8>>, Error> = db.get(b"k1");

        assert_eq!(r.unwrap().unwrap(), b"v1111");
        assert!(db.delete(b"k1").is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }
}

#[test]
fn transaction_test() {
    let path = DBPath::new("_rust_optimistic_transaction_db_transactiontest");
    {
        let db = OptimisticTransactionDB::open_default(&path).unwrap();

        let txn = db.transaction();

        txn.put(b"k1", b"v1").unwrap();
        txn.put(b"k2", b"v2").unwrap();
        txn.put(b"k3", b"v3").unwrap();
        txn.put(b"k4", b"v4").unwrap();

        assert_eq!(txn.commit().is_ok(), true);

        let txn2 = db.transaction();
        let txn3 = db.transaction();

        txn2.put(b"k2", b"v5").unwrap();
        txn3.put(b"k2", b"v6").unwrap();

        assert_eq!(txn3.commit().is_ok(), true);

        assert_eq!(txn2.commit().is_err(), true);
    }
}

fn to_u64<B: AsRef<[u8]>>(bytes: B) -> u64 {
    let array: [u8; 8] = bytes.as_ref().try_into().unwrap();
    u64::from_be_bytes(array)
}

fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old {
        Some(bytes) => {
            let number = to_u64(bytes);
            number + 1
        }
        None => 0,
    };

    Some(number.to_be_bytes().to_vec())
}

fn fetch_and_update<K: AsRef<[u8]>>(db: &OptimisticTransactionDB, key: K) -> Option<Vec<u8>> {
    let key_ref = key.as_ref();
    loop {
        let txn = db.transaction();
        let old = txn.get_for_update(key_ref).unwrap();
        let new = increment(old.as_ref().map(AsRef::as_ref));

        if let Some(vec) = new.clone() {
            txn.put(key_ref, vec).unwrap();
        } else {
            txn.delete(key_ref).unwrap();
        }

        if txn.commit().is_ok() {
            break new;
        }
    }
}

#[test]
fn fetch_and_update_test() {
    let path = DBPath::new("_rust_optimistic_transaction_db_fetch_and_updatetest");
    {
        let db = OptimisticTransactionDB::open_default(&path).unwrap();

        fetch_and_update(&db, b"test");

        let value = db.get(b"test").unwrap();
        assert_eq!(value.map(to_u64), Some(0));

        fetch_and_update(&db, b"test");
        fetch_and_update(&db, b"test");

        let value = db.get(b"test").unwrap();
        assert_eq!(value.map(to_u64), Some(2));
    }
}

#[test]
fn fetch_and_update_concurrent_test() {
    let path = DBPath::new("_rust_optimistic_transaction_db_fetch_and_update_concurrenttest");
    {
        let db = Arc::new(OptimisticTransactionDB::open_default(&path).unwrap());
        let n = 10;
        let barrier = Arc::new(std::sync::Barrier::new(n));

        let mut handles = vec![];
        for _i in 0..n {
            let db = db.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                fetch_and_update(&db, b"test")
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let value = db.get(b"test").unwrap();
        assert_eq!(value.map(to_u64), Some(n as u64 - 1));
    }
}

#[test]
fn create_and_drop_cf_test() {
    let path = DBPath::new("_rust_optimistic_transaction_db_create_and_drop_cftest");
    {
        let mut db = OptimisticTransactionDB::open_default(&path).unwrap();
        let opts = Options::default();
        db.create_cf("test_cf", &opts).unwrap();
        let cf = db.cf_handle("test_cf").unwrap();

        {
            let txn = db.transaction();
            let _ = txn.get_for_update_cf(cf, b"test").unwrap();

            db.put_cf(cf, b"test", "oops").unwrap();

            txn.put_cf(cf, b"test", "conflict").unwrap();

            assert_eq!(txn.commit().is_err(), true);
        }

        {
            let txn = db.transaction();
            let _ = txn.get_for_update_cf(cf, b"test").unwrap();
            txn.put_cf(cf, b"test", "no conflict").unwrap();

            assert_eq!(txn.commit().is_ok(), true);
        }

        db.drop_cf("test_cf").unwrap();

        assert_eq!(db.cf_handle("test_cf").is_none(), true);
    }
}

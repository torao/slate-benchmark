use std::collections::HashMap;
use std::fs::{remove_dir_all, remove_file};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rocksdb::{DB, DBCompressionType, Options};
use slate::rocksdb::RocksDBStorage;
use slate::{BlockStorage, Entry, FileStorage, Index, Position, Prove, Result, Slate, Storage};
use slate_benchmark::{MemKVS, file_size, splitmix64, unique_dir, unique_file};

use crate::{AppendCUT, CUT, Case, GetCUT, ProveCUT};

trait SlateCUT<S: Storage<Entry>>: CUT {
  fn create_new(case: &Case, id: &str) -> Result<(Self::T, Slate<S>)>;
  fn restore(target: &Self::T, cache_size: usize) -> Result<Slate<S>>;

  fn prepare<F: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: F) -> Result<Self::T> {
    let (target, mut slate) = Self::create_new(case, id)?;
    assert!(slate.n() == 0);
    while slate.n() < n {
      slate.append(&values(slate.n() + 1).to_le_bytes())?;
    }
    Ok(target)
  }

  #[inline(never)]
  fn gets<V: Fn(u64) -> u64>(
    target: &Self::T,
    is: &[Index],
    cache_size: usize,
    values: V,
  ) -> Result<Vec<(u64, Duration)>> {
    let slate = Self::restore(target, cache_size)?;
    let mut results = Vec::with_capacity(is.len());
    for i in is.iter().cloned() {
      assert!(slate.n() >= i, "n={} less than i={}", slate.n(), i);
      let start = Instant::now();
      let value = slate.snapshot().query()?.get(i)?;
      let elapsed = start.elapsed();
      assert_eq!(Some(values(i)), value.map(|b| u64::from_le_bytes(b.try_into().unwrap())));
      results.push((i, elapsed))
    }
    Ok(results)
  }

  #[inline(never)]
  fn append(target: &Self::T, n: Index) -> Result<Duration> {
    let mut slate = Self::restore(target, 0)?;
    assert!(slate.n() <= n);
    let start = Instant::now();
    while slate.n() < n {
      slate.append(&splitmix64(slate.n() + 1).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    Ok(elapse)
  }

  #[inline(never)]
  fn prove(path1: &Self::T, path2: &Self::T) -> Result<(Option<u64>, Duration)> {
    let db1 = Self::restore(path1, 0)?;
    let db2 = Self::restore(path2, 0)?;
    let mut query1 = db1.snapshot().query()?;
    let mut query2 = db2.snapshot().query()?;

    let start = Instant::now();
    let mut auth_path1 = query1.get_auth_path(db1.n())?.unwrap();
    let mut auth_path2 = query2.get_auth_path(db1.n())?.unwrap();
    let diff = loop {
      match auth_path2.prove(&auth_path1)? {
        Prove::Identical => break None,
        Prove::Divergent(divergents) => {
          let (min_i, min_j) = divergents.iter().min().unwrap();
          if *min_j == 0 {
            break Some(*min_i);
          }
          auth_path1 = query1.get_auth_path(*min_i)?.unwrap();
          auth_path2 = query2.get_auth_path(*min_i)?.unwrap();
        }
      }
    };
    let elapse = start.elapsed();
    Ok((diff, elapse))
  }
}

// --- MemKVS ---

#[derive(Default)]
pub struct MemSlateCUT {}

impl SlateCUT<MemKVS<Entry>> for MemSlateCUT {
  fn create_new(case: &Case, _id: &str) -> Result<(Self::T, Slate<MemKVS<Entry>>)> {
    let target = Arc::new(RwLock::new(HashMap::with_capacity(case.max_n as usize)));
    let storage = MemKVS::with_kvs(target.clone());
    let slate = Slate::new(storage)?;
    Ok((target, slate))
  }

  fn restore(target: &Self::T, _cache_size: usize) -> Result<Slate<MemKVS<Entry>>> {
    let storage = MemKVS::with_kvs(target.clone());
    let slate = Slate::new(storage)?;
    Ok(slate)
  }
}

impl CUT for MemSlateCUT {
  type T = Arc<RwLock<HashMap<Position, Entry>>>;

  fn prepare<T: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: T) -> Result<Self::T> {
    <Self as SlateCUT<MemKVS<Entry>>>::prepare(case, id, n, values)
  }

  fn remove(target: &Self::T) -> Result<()> {
    target.write()?.clear();
    Ok(())
  }
}

impl GetCUT for MemSlateCUT {
  fn gets<V: Fn(u64) -> u64>(
    target: &Self::T,
    is: &[Index],
    cache_size: usize,
    values: V,
  ) -> Result<Vec<(u64, Duration)>> {
    <Self as SlateCUT<MemKVS<Entry>>>::gets(target, is, cache_size, values)
  }
}

impl AppendCUT for MemSlateCUT {
  fn append(target: &Self::T, n: Index) -> Result<(u64, Duration)> {
    let time = <Self as SlateCUT<MemKVS<Entry>>>::append(target, n)?;
    Ok((0u64, time))
  }
}

impl ProveCUT for MemSlateCUT {
  fn prove(path1: &Self::T, path2: &Self::T) -> Result<(Option<u64>, Duration)> {
    <Self as SlateCUT<MemKVS<Entry>>>::prove(path1, path2)
  }
}

// --- File --

#[derive(Default)]
pub struct FileSlateCUT {}

impl CUT for FileSlateCUT {
  type T = PathBuf;
  fn prepare<F: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: F) -> Result<Self::T> {
    <Self as SlateCUT<FileStorage>>::prepare(case, id, n, values)
  }
  fn remove(path: &Self::T) -> Result<()> {
    if path.exists() {
      remove_file(path)?;
    }
    Ok(())
  }
}

impl SlateCUT<FileStorage> for FileSlateCUT {
  fn create_new(case: &Case, id: &str) -> Result<(Self::T, Slate<FileStorage>)> {
    let path = unique_file(&case.dir_work(id), "slate-file", ".db");
    let db = Slate::new_on_file(&path, false).unwrap();
    Ok((path, db))
  }

  fn restore(target: &Self::T, cache_size: usize) -> Result<Slate<FileStorage>> {
    let storage = BlockStorage::from_file(target, false)?;
    Slate::with_cache_level(storage, cache_size)
  }
}

impl GetCUT for FileSlateCUT {
  fn gets<V: Fn(u64) -> u64>(
    target: &Self::T,
    is: &[Index],
    cache_size: usize,
    values: V,
  ) -> Result<Vec<(u64, Duration)>> {
    <Self as SlateCUT<FileStorage>>::gets(target, is, cache_size, values)
  }
}

impl AppendCUT for FileSlateCUT {
  fn append(target: &Self::T, n: Index) -> Result<(u64, Duration)> {
    let time = <Self as SlateCUT<FileStorage>>::append(target, n)?;
    let size = file_size(target);
    Ok((size, time))
  }
}

impl ProveCUT for FileSlateCUT {
  fn prove(path1: &Self::T, path2: &Self::T) -> Result<(Option<u64>, Duration)> {
    <Self as SlateCUT<FileStorage>>::prove(path1, path2)
  }
}

// --- RocksDB ---

#[derive(Default)]
pub struct RocksDBSlateCUT {}

impl CUT for RocksDBSlateCUT {
  type T = PathBuf;
  fn prepare<F: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: F) -> Result<Self::T> {
    <Self as SlateCUT<RocksDBStorage>>::prepare(case, id, n, values)
  }
  fn remove(path: &Self::T) -> Result<()> {
    if path.exists() {
      remove_dir_all(path)?;
    }
    Ok(())
  }
}

impl SlateCUT<RocksDBStorage> for RocksDBSlateCUT {
  fn create_new(case: &Case, id: &str) -> Result<(Self::T, Slate<RocksDBStorage>)> {
    let lock = unique_dir(&case.dir_work(id), "slate-rocksdb", "");
    let path = case.dir_work(id).join(format!("{}.db", lock.file_name().unwrap().to_string_lossy()));
    assert!(!path.exists());
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_compression_per_level(&[DBCompressionType::None; 7]);
    let db = Arc::new(RwLock::new(DB::open(&opts, &path).unwrap()));
    let storage = RocksDBStorage::new(db, &[], false);
    let db = Slate::new(storage)?;
    Ok((path, db))
  }

  fn restore(target: &Self::T, cache_size: usize) -> Result<Slate<RocksDBStorage>> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_compression_per_level(&[DBCompressionType::None; 7]);
    let db = Arc::new(RwLock::new(DB::open(&opts, target).unwrap()));
    let storage = RocksDBStorage::new(db, &[], false);
    let db = Slate::with_cache_level(storage, cache_size)?;
    Ok(db)
  }
}

impl GetCUT for RocksDBSlateCUT {
  fn gets<V: Fn(u64) -> u64>(
    target: &Self::T,
    is: &[Index],
    cache_size: usize,
    values: V,
  ) -> Result<Vec<(u64, Duration)>> {
    <Self as SlateCUT<RocksDBStorage>>::gets(target, is, cache_size, values)
  }
}

impl AppendCUT for RocksDBSlateCUT {
  fn append(target: &Self::T, n: Index) -> Result<(u64, Duration)> {
    let time = <Self as SlateCUT<RocksDBStorage>>::append(target, n)?;
    let size = file_size(target);
    Ok((size, time))
  }
}

impl ProveCUT for RocksDBSlateCUT {
  fn prove(path1: &Self::T, path2: &Self::T) -> Result<(Option<u64>, Duration)> {
    <Self as SlateCUT<RocksDBStorage>>::prove(path1, path2)
  }
}

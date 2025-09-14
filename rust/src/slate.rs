use std::collections::HashMap;
use std::fs::{remove_dir_all, remove_file};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rocksdb::{DB, DBCompressionType, Options};
use slate::rocksdb::RocksDBStorage;
use slate::{Entry, FileStorage, Index, Position, Prove, Result, Slate, Storage};
use slate_benchmark::{MemKVS, file_size, unique_file};

use crate::{AppendCUT, CUT, GetCUT, ProveCUT};

pub trait StorageFactory<S: Storage<Entry>> {
  fn name() -> String;
  fn new_storage(&self) -> Result<S>;
  fn storage_size(&self) -> Result<u64>;
  fn clear(&mut self) -> Result<()>;
  fn alternate(&self) -> Result<Self>
  where
    Self: std::marker::Sized;
}

pub struct SlateCUT<S: Storage<Entry>, F: StorageFactory<S>> {
  factory: Option<F>,
  slate: Option<Slate<S>>,
  _phantom: PhantomData<S>,
}

impl<S: Storage<Entry>, F: StorageFactory<S>> SlateCUT<S, F> {
  pub fn new(factory: F) -> Result<Self> {
    let storage = factory.new_storage()?;
    let slate = Some(Slate::with_cache_level(storage, 0)?);
    let factory = Some(factory);
    Ok(Self { factory, slate, _phantom: PhantomData })
  }
}

impl<S: Storage<Entry>, F: StorageFactory<S>> Drop for SlateCUT<S, F> {
  fn drop(&mut self) {
    drop(self.slate.take());
    drop(self.factory.take());
  }
}

impl<S: Storage<Entry>, F: StorageFactory<S>> CUT for SlateCUT<S, F> {
  fn implementation(&self) -> String {
    F::name()
  }
}

impl<S: Storage<Entry>, F: StorageFactory<S>> AppendCUT for SlateCUT<S, F> {
  #[inline(never)]
  fn append<V: Fn(u64) -> u64>(&mut self, n: Index, values: V) -> Result<(u64, Duration)> {
    let slate = self.slate.as_mut().unwrap();
    assert!(slate.n() <= n);
    let start = Instant::now();
    while slate.n() < n {
      slate.append(&values(slate.n() + 1).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    let size = self.factory.as_ref().unwrap().storage_size()?;
    Ok((size, elapse))
  }

  fn clear(&mut self) -> Result<()> {
    drop(self.slate.take());
    self.factory.as_mut().unwrap().clear()?;
    let storage = self.factory.as_ref().unwrap().new_storage()?;
    self.slate = Some(Slate::with_cache_level(storage, 0)?);
    Ok(())
  }
}

impl<S: Storage<Entry>, F: StorageFactory<S>> GetCUT for SlateCUT<S, F> {
  fn set_cache_level(&mut self, cache_level: usize) -> Result<()> {
    if self.slate.as_ref().unwrap().cache().level() != cache_level {
      drop(self.slate.take());
      let storage = self.factory.as_ref().unwrap().new_storage()?;
      self.slate = Some(Slate::with_cache_level(storage, cache_level)?);
    }
    Ok(())
  }

  fn prepare<V: Fn(u64) -> u64>(&mut self, n: Index, values: V) -> Result<()> {
    let slate = self.slate.as_mut().unwrap();
    if slate.n() != n {
      assert!(slate.n() < n, "slate {} is larger than {n}", slate.n());
      while slate.n() < n {
        let n = slate.n() + 1;
        slate.append(&values(n).to_le_bytes())?;
      }
    }
    Ok(())
  }

  #[inline(never)]
  fn get<V: Fn(u64) -> u64>(&mut self, i: Index, values: V) -> Result<Duration> {
    let slate = self.slate.as_mut().unwrap();
    assert!(slate.n() >= i, "n={} less than i={}", slate.n(), i);
    let start = Instant::now();
    let value = slate.snapshot().query()?.get(i)?;
    let elapsed = start.elapsed();
    assert_eq!(Some(values(i)), value.map(|b| u64::from_le_bytes(b.try_into().unwrap())));
    Ok(elapsed)
  }
}

impl<S, F> ProveCUT for SlateCUT<S, F>
where
  S: Storage<Entry> + Sync + Send,
  F: StorageFactory<S> + Sync + Send,
{
  #[inline(never)]
  fn prove(&self, other: &Self) -> Result<(Option<u64>, Duration)> {
    let slate1 = self.slate.as_ref().unwrap();
    let slate2 = other.slate.as_ref().unwrap();
    let mut query1 = slate1.snapshot().query()?;
    let mut query2 = slate2.snapshot().query()?;

    let start = Instant::now();
    let mut auth_path1 = query1.get_auth_path(slate1.n())?.unwrap();
    let mut auth_path2 = query2.get_auth_path(slate2.n())?.unwrap();
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

  fn alternate(&self) -> Result<Self> {
    Self::new(self.factory.as_ref().unwrap().alternate()?)
  }
}

// --- MemKVS ---

pub struct MemKVSFactory {
  cache: Arc<RwLock<HashMap<Position, Entry>>>,
}

impl MemKVSFactory {
  pub fn new(capacity: usize) -> Self {
    let cache = Arc::new(RwLock::new(HashMap::with_capacity(capacity)));
    Self { cache }
  }
}

impl StorageFactory<MemKVS<Entry>> for MemKVSFactory {
  fn name() -> String {
    String::from("slate-memkvs")
  }

  fn new_storage(&self) -> Result<MemKVS<Entry>> {
    Ok(MemKVS::with_kvs(self.cache.clone()))
  }

  fn storage_size(&self) -> Result<u64> {
    Ok(0u64)
  }

  fn clear(&mut self) -> Result<()> {
    self.cache.write()?.clear();
    Ok(())
  }

  fn alternate(&self) -> Result<Self> {
    Ok(Self::new(self.cache.read()?.capacity()))
  }
}

// --- File --

pub struct FileFactory {
  path: PathBuf,
}

impl FileFactory {
  pub fn new(dir: &Path) -> Self {
    let path = unique_file(dir, &Self::name(), ".db");
    Self { path }
  }
}

impl Drop for FileFactory {
  fn drop(&mut self) {
    if let Err(e) = self.clear() {
      eprintln!("WARN: Failed to delete file {:?}: {}", self.path, e);
    }
  }
}

impl StorageFactory<FileStorage> for FileFactory {
  fn name() -> String {
    String::from("slate-file")
  }

  fn new_storage(&self) -> Result<FileStorage> {
    FileStorage::from_file(&self.path, false)
  }

  fn storage_size(&self) -> Result<u64> {
    Ok(file_size(&self.path))
  }

  fn clear(&mut self) -> Result<()> {
    if self.path.exists() {
      remove_file(&self.path)?;
    }
    Ok(())
  }

  fn alternate(&self) -> Result<Self> {
    Ok(Self::new(&PathBuf::from(self.path.parent().unwrap())))
  }
}

// --- RocksDB ---

pub struct RocksDBFactory {
  lock_file: PathBuf,
}

impl RocksDBFactory {
  pub fn new(dir: &Path) -> Self {
    let lock_file = unique_file(dir, &Self::name(), ".lock");
    assert!(lock_file.is_file());
    Self { lock_file }
  }

  pub fn data_dir(&self) -> PathBuf {
    let mut dir = self.lock_file.clone();
    dir.set_extension("db");
    dir
  }
}

impl Drop for RocksDBFactory {
  fn drop(&mut self) {
    if let Err(e) = self.clear() {
      eprintln!("WARN: Failed to delete directory {:?}: {}", self.data_dir(), e);
    }
    if self.lock_file.exists() {
      if let Err(e) = remove_file(&self.lock_file) {
        eprintln!("WARN: Failed to delete file {:?}: {}", self.lock_file, e);
      }
    }
  }
}

impl StorageFactory<RocksDBStorage> for RocksDBFactory {
  fn name() -> String {
    String::from("slate-rocksdb")
  }

  fn new_storage(&self) -> Result<RocksDBStorage> {
    let path = self.data_dir();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_compression_per_level(&[DBCompressionType::None; 7]);
    match DB::open(&opts, &path) {
      Ok(db) => {
        let db = Arc::new(RwLock::new(db));
        Ok(RocksDBStorage::new(db, &[], false))
      }
      Err(err) => {
        eprintln!("ERROR: fail to open RocksDB: {path:?}");
        Err(err)?
      }
    }
  }

  fn storage_size(&self) -> Result<u64> {
    Ok(file_size(self.data_dir()))
  }

  fn clear(&mut self) -> Result<()> {
    let dir = self.data_dir();
    if dir.exists() {
      remove_dir_all(&dir)?;
    }
    Ok(())
  }

  fn alternate(&self) -> Result<Self> {
    Ok(Self::new(&PathBuf::from(self.lock_file.parent().unwrap())))
  }
}

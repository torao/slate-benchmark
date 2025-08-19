use std::fs::{remove_dir_all, remove_file};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rocksdb::{DB, DBCompressionType, Options};
use slate::file::FileDevice;
use slate::memory::MemoryDevice;
use slate::rocksdb::RocksDBStorage;
use slate::{BlockStorage, Entry, Index, Result, Slate, Storage};
use slate_benchmark::{MemKVS, splitmix64};

use crate::{Case, Driver};

pub struct MemoryAppendDriver {}
impl MemoryAppendDriver {
  pub fn new() -> Self {
    MemoryAppendDriver {}
  }
}
impl Driver<Slate<BlockStorage<MemoryDevice>>, Duration> for MemoryAppendDriver {
  fn setup(&mut self, _case: &Case) -> Result<Slate<BlockStorage<MemoryDevice>>> {
    Ok(Slate::new_on_memory_with_capacity(60 * _case.max_n as usize))
  }
  #[inline(never)]
  fn run(&mut self, _case: &Case, db: &mut Slate<BlockStorage<MemoryDevice>>, n: u64) -> Result<Duration> {
    let start = Instant::now();
    for i in 1..=n {
      db.append(&splitmix64(i).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    Ok(elapse)
  }
}

pub struct FileAppendDriver {
  path: Option<PathBuf>,
}
impl FileAppendDriver {
  pub fn new() -> Self {
    FileAppendDriver { path: None }
  }
}
impl Driver<Slate<BlockStorage<FileDevice>>, Duration> for FileAppendDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<BlockStorage<FileDevice>>> {
    let path = case.file("slate-file.db");
    self.path = Some(path.clone());
    Slate::new_on_file(&path, false)
  }
  #[inline(never)]
  fn run(&mut self, _case: &Case, db: &mut Slate<BlockStorage<FileDevice>>, n: u64) -> Result<Duration> {
    let start = Instant::now();
    for i in 1..=n {
      db.append(&splitmix64(i).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    Ok(elapse)
  }
  fn cleanup(&mut self, _case: &Case, db: Slate<BlockStorage<FileDevice>>) -> Result<()> {
    drop(db);
    if let Some(path) = &self.path {
      remove_file(path)?;
    }
    self.path = None;
    Ok(())
  }
}

pub struct RocksDBAppendDriver {
  dir: Option<PathBuf>,
}
impl RocksDBAppendDriver {
  pub fn new() -> Self {
    RocksDBAppendDriver { dir: None }
  }
}
impl Driver<Slate<RocksDBStorage>, Duration> for RocksDBAppendDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<RocksDBStorage>> {
    let dir = case.file("slate-rocksdb.db");
    self.dir = Some(dir.clone());

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_compression_per_level(&[DBCompressionType::None; 7]);
    let db = Arc::new(RwLock::new(DB::open(&opts, &dir).unwrap()));
    let storage = RocksDBStorage::new(db, &[], false);
    Slate::new(storage)
  }
  #[inline(never)]
  fn run(&mut self, _case: &Case, db: &mut Slate<RocksDBStorage>, n: u64) -> Result<Duration> {
    let start = Instant::now();
    for i in 1..=n {
      db.append(&splitmix64(i).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    Ok(elapse)
  }
  fn cleanup(&mut self, _case: &Case, db: Slate<RocksDBStorage>) -> Result<()> {
    drop(db);
    if let Some(dir) = &self.dir {
      remove_dir_all(dir)?;
    }
    self.dir = None;
    Ok(())
  }
}

pub struct MemKVSQueryDriver {}
impl MemKVSQueryDriver {
  pub fn new() -> Self {
    Self {}
  }
}
impl Driver<Slate<MemKVS<Entry>>, Duration> for MemKVSQueryDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<MemKVS<Entry>>> {
    let storage = MemKVS::new();
    let mut db = Slate::new(storage)?;
    ensure(&mut db, case.max_n)?;
    assert_eq!(case.max_n, db.n());
    Ok(db)
  }
  fn run(&mut self, case: &Case, db: &mut Slate<MemKVS<Entry>>, i: u64) -> Result<Duration> {
    run_query(case, db, i)
  }
}

pub struct FileQueryDriver {
  path: Option<PathBuf>,
}
impl FileQueryDriver {
  pub fn new() -> Self {
    FileQueryDriver { path: None }
  }
}
impl Driver<Slate<BlockStorage<FileDevice>>, Duration> for FileQueryDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<BlockStorage<FileDevice>>> {
    let path = case.file(&format!("slate-file-{}.db", case.max_n));
    self.path = Some(path.clone());
    let mut db = open(&path)?;
    ensure(&mut db, case.max_n)?;
    assert_eq!(case.max_n, db.n());
    Ok(db)
  }
  fn run(&mut self, case: &Case, db: &mut Slate<BlockStorage<FileDevice>>, i: u64) -> Result<Duration> {
    run_query(case, db, i)
  }
  fn cleanup(&mut self, _case: &Case, _db: Slate<BlockStorage<FileDevice>>) -> Result<()> {
    self.path = None;
    Ok(())
  }
}

pub struct RocksDBQueryDriver {
  dir: Option<PathBuf>,
}
impl RocksDBQueryDriver {
  pub fn new() -> Self {
    Self { dir: None }
  }
}
impl Driver<Slate<RocksDBStorage>, Duration> for RocksDBQueryDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<RocksDBStorage>> {
    let dir = case.file("slate-rocksdb.db");
    self.dir = Some(dir.clone());

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_compression_per_level(&[DBCompressionType::None; 7]);
    let db = Arc::new(RwLock::new(DB::open(&opts, &dir).unwrap()));
    let storage = RocksDBStorage::new(db, &[], false);
    let mut db = Slate::new(storage)?;
    ensure(&mut db, case.max_n)?;
    assert_eq!(case.max_n, db.n());
    Ok(db)
  }
  fn run(&mut self, case: &Case, db: &mut Slate<RocksDBStorage>, i: u64) -> Result<Duration> {
    run_query(case, db, i)
  }
  fn cleanup(&mut self, _case: &Case, _db: Slate<RocksDBStorage>) -> Result<()> {
    self.dir = None;
    Ok(())
  }
}

#[inline(never)]
fn run_query<S: Storage<Entry>>(_case: &Case, db: &mut Slate<S>, i: u64) -> Result<Duration> {
  let start = Instant::now();
  let snapshot = db.snapshot();
  let mut query = snapshot.query()?;
  let value: [u8; 8] = query.get(i)?.unwrap().try_into().unwrap();
  let elapse = start.elapsed();
  if splitmix64(i) != u64::from_le_bytes(value) {
    panic!();
  }
  Ok(elapse)
}

pub struct FileVolumeDriver {
  path: Option<PathBuf>,
}
impl FileVolumeDriver {
  pub fn new() -> Self {
    FileVolumeDriver { path: None }
  }
}
impl Driver<Slate<BlockStorage<FileDevice>>, u64> for FileVolumeDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<BlockStorage<FileDevice>>> {
    let path = case.file(&format!("slate-file-{}.db", case.max_n));
    self.path = Some(path.clone());
    open(&path)
  }
  #[inline(never)]
  fn run(&mut self, _case: &Case, db: &mut Slate<BlockStorage<FileDevice>>, n: u64) -> Result<u64> {
    ensure(db, n)?;
    if db.n() == n {
      let path = db.storage().device().path();
      Ok(path.metadata()?.len())
    } else {
      let snapshot = db.snapshot();
      let mut query = snapshot.query().unwrap();
      let entry = query.read_entry(n + 1)?.unwrap();
      Ok(entry.root().address.position)
    }
  }
}

pub struct FileCacheDriver {
  level: usize,
}
impl FileCacheDriver {
  pub fn new(level: usize) -> Self {
    Self { level }
  }
}
impl Driver<Slate<BlockStorage<FileDevice>>, Duration> for FileCacheDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<BlockStorage<FileDevice>>> {
    let path = case.file(&format!("slate-file-{}.db", case.max_n));
    let storage = BlockStorage::from_file(&path, false)?;
    let mut db = Slate::with_cache_level(storage, self.level)?;
    ensure(&mut db, case.max_n)?;
    Ok(db)
  }
  #[inline(never)]
  fn run(&mut self, case: &Case, db: &mut Slate<BlockStorage<FileDevice>>, i: u64) -> Result<Duration> {
    run_query(case, db, i)
  }
}

/// 既存のファイルをオープンします。中断などによりファイルが葉損している場合は新しく開き直します。
fn open(path: &Path) -> Result<Slate<BlockStorage<FileDevice>>> {
  match Slate::new_on_file(path, false) {
    Ok(db) => Ok(db),
    Err(_) => {
      println!("WARN: removing incomplete file: {}", path.to_string_lossy());
      remove_file(path)?;
      Slate::new_on_file(path, false)
    }
  }
}

/// 指定されたストレージのデータ量を保証します。
fn ensure<S: Storage<Entry>>(db: &mut Slate<S>, n: Index) -> Result<()> {
  while db.n() < n {
    db.append(&splitmix64(db.n() + 1).to_le_bytes())?;
  }
  Ok(())
}

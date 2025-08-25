use std::fs::{OpenOptions, create_dir, remove_dir_all, remove_file};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rayon::iter::split;
use rocksdb::{DB, DBCompressionType, Options};
use slate::file::FileDevice;
use slate::memory::MemoryDevice;
use slate::rocksdb::RocksDBStorage;
use slate::{BlockStorage, Entry, FileStorage, Index, Prove, Result, Slate, Storage};
use slate_benchmark::{MemKVS, file_size, splitmix64};

use crate::{AppendCUT, CUT, Case, Driver, ProveCUT};

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

pub struct RocksDBVolumeDriver {
  dir: Option<PathBuf>,
}
impl RocksDBVolumeDriver {
  pub fn new() -> Self {
    Self { dir: None }
  }
}
impl Driver<Slate<RocksDBStorage>, u64> for RocksDBVolumeDriver {
  fn setup(&mut self, case: &Case) -> Result<Slate<RocksDBStorage>> {
    let dir = case.file(&format!("slate-rocksdb-{}.db", case.max_n));
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
  fn run(&mut self, _case: &Case, db: &mut Slate<RocksDBStorage>, n: u64) -> Result<u64> {
    ensure(db, n)?;
    if db.n() > n {
      panic!("{n} expected, but {}", db.n());
    }
    Ok(file_size(self.dir.clone().unwrap()))
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

trait SlateCUT<S: Storage<Entry>>: CUT {
  fn create_new(case: &Case) -> Result<(Self::T, Slate<S>)>;
  fn restore(target: &Self::T) -> Result<Slate<S>>;

  fn prepare<F: Fn(Index) -> u64>(case: &Case, n: Index, values: F) -> Result<Self::T> {
    let (target, mut slate) = Self::create_new(case)?;
    assert!(slate.n() == 0);
    while slate.n() < n {
      slate.append(&values(slate.n() + 1).to_le_bytes())?;
    }
    Ok(target)
  }

  #[inline(never)]
  fn append(target: &Self::T, n: Index) -> Result<Duration> {
    let mut slate = Self::restore(target)?;
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
    let db1 = Self::restore(path1)?;
    let db2 = Self::restore(path2)?;
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

#[derive(Default)]
pub struct FileSlateCUT {}

impl CUT for FileSlateCUT {
  type T = PathBuf;
  fn prepare<F: Fn(Index) -> u64>(case: &Case, n: Index, values: F) -> Result<Self::T> {
    <Self as SlateCUT<FileStorage>>::prepare(case, n, values)
  }
  fn remove(path: &Self::T) -> Result<()> {
    if path.exists() {
      remove_file(path)?;
    }
    Ok(())
  }
}

impl SlateCUT<FileStorage> for FileSlateCUT {
  fn create_new(case: &Case) -> Result<(Self::T, Slate<FileStorage>)> {
    let path = unique_file(&case.dir_work, "slate-file", ".db");
    let db = Slate::new_on_file(&path, false).unwrap();
    Ok((path, db))
  }

  fn restore(target: &Self::T) -> Result<Slate<FileStorage>> {
    Slate::new_on_file(target, false)
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

#[derive(Default)]
pub struct RocksDBSlateCUT {}

impl CUT for RocksDBSlateCUT {
  type T = PathBuf;
  fn prepare<F: Fn(Index) -> u64>(case: &Case, n: Index, values: F) -> Result<Self::T> {
    <Self as SlateCUT<RocksDBStorage>>::prepare(case, n, values)
  }
  fn remove(path: &Self::T) -> Result<()> {
    if path.exists() {
      remove_dir_all(path)?;
    }
    Ok(())
  }
}

impl SlateCUT<RocksDBStorage> for RocksDBSlateCUT {
  fn create_new(case: &Case) -> Result<(Self::T, Slate<RocksDBStorage>)> {
    let lock = unique_dir(&case.dir_work, "slate-rocksdb", "");
    let path = case.dir_work.join(format!("{}.db", lock.file_name().unwrap().to_string_lossy()));
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

  fn restore(target: &Self::T) -> Result<Slate<RocksDBStorage>> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_compression_type(DBCompressionType::None);
    opts.set_compression_per_level(&[DBCompressionType::None; 7]);
    let db = Arc::new(RwLock::new(DB::open(&opts, target).unwrap()));
    let storage = RocksDBStorage::new(db, &[], false);
    let db = Slate::new(storage)?;
    Ok(db)
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

fn unique_file(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
  for i in 0..=usize::MAX {
    let name = if i == 0 { format!("{prefix}{suffix}") } else { format!("{prefix}_{i}{suffix}") };
    let path = dir.join(name);
    if OpenOptions::new().write(true).create_new(true).open(&path).is_ok() {
      return path;
    }
  }
  panic!("Temporary file name space is full: {prefix}_nnn{suffix}");
}

fn unique_dir(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
  for i in 0..=usize::MAX {
    let name = if i == 0 { format!("{prefix}{suffix}") } else { format!("{prefix}_{i}{suffix}") };
    let path = dir.join(name);
    match create_dir(&path) {
      Ok(()) => return path,
      Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
      Err(e) => panic!("Error: {e}"),
    }
  }
  panic!("Temporary file name space is full: {prefix}_nnn{suffix}");
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

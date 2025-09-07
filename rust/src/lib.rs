use std::collections::HashMap;
use std::fs::{OpenOptions, create_dir, metadata, read_dir};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use slate::{Position, Result, Serializable, Storage};

pub mod hashtree;

pub struct MemKVS<S: Serializable + Clone + 'static> {
  kvs: Arc<RwLock<HashMap<Position, S>>>,
}

struct MemKVSReader<S: Serializable + 'static> {
  kvs: Arc<RwLock<HashMap<Position, S>>>,
}

impl<S: Serializable + Clone + 'static> MemKVS<S> {
  pub fn new() -> Self {
    Self::with_kvs(Default::default())
  }

  pub fn with_kvs(kvs: Arc<RwLock<HashMap<Position, S>>>) -> Self {
    Self { kvs }
  }
}

impl<S: Serializable + Clone + 'static> Default for MemKVS<S> {
  fn default() -> Self {
    Self::new()
  }
}

impl<S: Serializable + Clone + 'static> Storage<S> for MemKVS<S> {
  fn first(&mut self) -> Result<(Option<S>, slate::Position)> {
    let kvs = self.kvs.read()?;
    let n = kvs.len() as Position;
    Ok((kvs.get(&n).cloned(), n + 1))
  }

  fn last(&mut self) -> Result<(Option<S>, slate::Position)> {
    let kvs = self.kvs.read()?;
    let n = kvs.len() as Position;
    if n == 0 { Ok((None, 1)) } else { Ok((kvs.get(&n).cloned(), n + 1)) }
  }

  fn put(&mut self, position: Position, data: &S) -> Result<slate::Position> {
    let mut kvs = self.kvs.write()?;
    kvs.insert(position, data.clone());
    Ok(kvs.len() as Position + 1)
  }

  fn reader(&self) -> Result<Box<dyn slate::Reader<S>>> {
    Ok(Box::new(MemKVSReader { kvs: self.kvs.clone() }))
  }
}

impl<S: Serializable + Clone> slate::Reader<S> for MemKVSReader<S> {
  fn read(&mut self, position: Position) -> Result<S> {
    let kvs = self.kvs.read()?;
    Ok(kvs.get(&position).cloned().unwrap())
  }
}

pub fn unique_file(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
  for i in 0..=usize::MAX {
    let name = if i == 0 { format!("{prefix}{suffix}") } else { format!("{prefix}_{i}{suffix}") };
    let path = dir.join(name);
    if OpenOptions::new().write(true).create_new(true).open(&path).is_ok() {
      return path;
    }
  }
  panic!("Temporary file name space is full: {prefix}_nnn{suffix}");
}

pub fn unique_dir(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
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

pub fn file_size<P: AsRef<Path>>(path: P) -> u64 {
  if path.as_ref().is_file() {
    metadata(&path).map(|m| m.len()).unwrap_or(0)
  } else if path.as_ref().is_dir() {
    read_dir(path)
      .unwrap()
      .flat_map(std::result::Result::ok)
      .map(|e| {
        let path = e.path();
        if path.is_dir() { file_size(&path) } else { metadata(&path).map(|m| m.len()).unwrap_or(0) }
      })
      .sum()
  } else {
    0
  }
}

pub fn file_count_and_size<P: AsRef<Path>>(path: P) -> (usize, u64) {
  if path.as_ref().is_file() {
    (1, metadata(&path).map(|m| m.len()).unwrap_or(0))
  } else if path.as_ref().is_dir() {
    read_dir(path)
      .unwrap()
      .flat_map(std::result::Result::ok)
      .map(|e| {
        let path = e.path();
        if path.is_dir() { file_size(&path) } else { metadata(&path).map(|m| m.len()).unwrap_or(0) }
      })
      .fold((0, 0), |(c, s), x| (c + 1, s + x))
  } else {
    (0, 0)
  }
}

pub fn splitmix64(x: u64) -> u64 {
  let mut z = x;
  z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
  z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
  z ^ (z >> 31)
}

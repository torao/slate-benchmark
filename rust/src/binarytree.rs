use std::fs::remove_file;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use slate::Index;
use slate::Result;
use slate_benchmark::hashtree::{HashTree, binary::BinaryHashTree};
use slate_benchmark::unique_file;

use crate::{CUT, GetCUT};

#[derive(Default)]
pub struct FileBinaryTreeCUT {
  path: PathBuf,
  cache_level: usize,
}

impl FileBinaryTreeCUT {
  pub fn new(dir: &Path, n: u64) -> Result<Self> {
    assert_eq!((n & (n - 1)), 0, "must be binary");
    let path = unique_file(dir, "hashtree-file", ".db");
    let cache_level = 0;
    Ok(Self { path, cache_level })
  }
}

impl Drop for FileBinaryTreeCUT {
  fn drop(&mut self) {
    if self.path.exists() {
      if let Err(e) = remove_file(&self.path) {
        eprintln!("WARN: fail to remove file {:?}: {}", self.path, e);
      }
    }
  }
}

impl CUT for FileBinaryTreeCUT {
  fn implementation(&self) -> String {
    String::from("hashtree-file")
  }
}

impl GetCUT for FileBinaryTreeCUT {
  #[inline(never)]
  fn get<V: Fn(u64) -> u64>(&mut self, i: Index, values: V) -> Result<Duration> {
    let mut bht = BinaryHashTree::from_file(&self.path, 1 << self.cache_level)?;
    let start = Instant::now();
    let value = bht.get(i)?;
    let elapsed = start.elapsed();
    assert_eq!(Some(values(i)), value.map(|b| u64::from_le_bytes(b.try_into().unwrap())), " at {i}");
    Ok(elapsed)
  }

  fn set_cache_level(&mut self, cache_size: usize) -> Result<()> {
    self.cache_level = cache_size;
    Ok(())
  }

  fn prepare<V: Fn(u64) -> u64, P: Fn(Index)>(&mut self, n: Index, values: V, progress: P) -> Result<()> {
    assert_eq!((n & (n - 1)), 0, "must be binary");
    BinaryHashTree::create_on_file(&self.path, u64::ilog2(n) as u8 + 1, 1 << self.cache_level, |i| {
      let bytes = values(i).to_le_bytes().to_vec();
      (progress)(1);
      bytes
    })?;
    Ok(())
  }
}

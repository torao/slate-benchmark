use std::fs::remove_file;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use slate::Index;
use slate::Result;
use slate_benchmark::hashtree::{HashTree, binary::BinaryHashTree};
use slate_benchmark::unique_file;

use crate::{CUT, Case, GetCUT};

#[derive(Default)]
pub struct FileBinaryTreeCUT {}

impl CUT for FileBinaryTreeCUT {
  type T = PathBuf;

  fn prepare<T: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: T) -> Result<Self::T> {
    assert_eq!((n & (n - 1)), 0, "must be binary");
    let target = unique_file(&case.dir_work(id), "hashtree-file", ".db");
    BinaryHashTree::create_on_file(&target, u64::ilog2(n) as u8 + 1, 0, |i| values(i).to_le_bytes().to_vec())?;
    Ok(target)
  }

  fn remove(target: &Self::T) -> Result<()> {
    remove_file(target)?;
    Ok(())
  }
}

impl GetCUT for FileBinaryTreeCUT {
  #[inline(never)]
  fn gets<V: Fn(u64) -> u64>(
    target: &Self::T,
    is: &[Index],
    cache_size: usize,
    values: V,
  ) -> Result<Vec<(u64, Duration)>> {
    let mut bht = BinaryHashTree::from_file(target, cache_size)?;
    let mut results = Vec::with_capacity(is.len());
    for i in is.iter().cloned() {
      let start = Instant::now();
      let value = bht.get(i)?;
      let elapsed = start.elapsed();
      assert_eq!(Some(values(i)), value.map(|b| u64::from_le_bytes(b.try_into().unwrap())), " at {i}");
      results.push((i, elapsed))
    }
    Ok(results)
  }
}

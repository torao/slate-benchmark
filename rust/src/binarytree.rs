use std::time::{Duration, Instant};

use slate::{BlockStorage, Result, file::FileDevice, formula::ceil_log2};
use slate_benchmark::hashtree::{HashTree, binary::BinaryHashTree};
use slate_benchmark::splitmix64;

use crate::{Case, Driver};

pub struct BinTreeQueryDiver {}
impl BinTreeQueryDiver {
  pub fn new() -> Self {
    Self {}
  }
}
impl Driver<BinaryHashTree<BlockStorage<FileDevice>>, Duration> for BinTreeQueryDiver {
  fn setup(&mut self, case: &Case) -> Result<BinaryHashTree<BlockStorage<FileDevice>>> {
    assert_eq!((case.max_n & (case.max_n - 1)), 0, "must be binary");
    let height = ceil_log2(case.max_n) + 1;
    let path = case.file(&format!("binary-tree-{height}.db"));
    let db = if path.exists() {
      BinaryHashTree::from_file(&path, height as usize)?
    } else {
      BinaryHashTree::create_on_file(&path, height, height as usize)?
    };
    assert_eq!(case.max_n, db.size());
    Ok(db)
  }

  fn run(&mut self, _case: &Case, db: &mut BinaryHashTree<BlockStorage<FileDevice>>, i: u64) -> Result<Duration> {
    let start = Instant::now();
    let value: [u8; 8] = db.get(i)?.unwrap().try_into().unwrap();
    let elapse = start.elapsed();
    if splitmix64(i) != u64::from_le_bytes(value) {
      panic!();
    }
    Ok(elapse)
  }
}

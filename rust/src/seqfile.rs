use slate::{Index, Result};
use slate_benchmark::{file_size, splitmix64, unique_file};
use std::fs::{OpenOptions, remove_file};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::{AppendCUT, CUT, Case};

#[derive(Default)]
pub struct SeqFileCUT {}

impl CUT for SeqFileCUT {
  type T = PathBuf;

  fn prepare<T: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: T) -> Result<Self::T> {
    let path = unique_file(&case.dir_work(id), "seqfile", ".db");
    let mut file = OpenOptions::new().create_new(false).append(false).write(true).open(&path)?;
    for i in 1..=n {
      file.write_all(&values(i).to_le_bytes())?;
    }
    Ok(path)
  }

  fn remove(path: &Self::T) -> Result<()> {
    if path.exists() {
      remove_file(path)?;
    }
    Ok(())
  }
}

impl AppendCUT for SeqFileCUT {
  #[inline(never)]
  fn append(path: &Self::T, n: Index) -> Result<(u64, Duration)> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    let begin = file.metadata()?.len() / 8;
    assert!(begin <= n);
    let start = Instant::now();
    for i in begin + 1..=n {
      file.write_all(&splitmix64(i).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    let size = file_size(path);
    Ok((size, elapse))
  }
}

use slate::{Index, Result};
use slate_benchmark::unique_file;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::{AppendCUT, CUT, GetCUT};

pub struct SeqFileCUT {
  path: PathBuf,
  file: Option<File>,
  cache_level: usize,
}

impl SeqFileCUT {
  pub fn new(dir: &Path) -> Result<Self> {
    let path = unique_file(dir, "seqfile", ".db");
    let file = Some(OpenOptions::new().create_new(false).append(false).read(true).write(true).open(&path)?);
    let cache_level = 0;
    Ok(Self { path, file, cache_level })
  }
}

impl Drop for SeqFileCUT {
  fn drop(&mut self) {
    drop(self.file.take());
    if self.path.exists() {
      if let Err(e) = remove_file(&self.path) {
        eprintln!("WARN: fail to remove file {:?}: {}", self.path, e);
      }
    }
  }
}

impl CUT for SeqFileCUT {
  fn implementation(&self) -> String {
    String::from("seqfile-file")
  }
}

impl GetCUT for SeqFileCUT {
  fn set_cache_level(&mut self, cache_size: usize) -> Result<()> {
    self.cache_level = cache_size;
    Ok(())
  }

  fn prepare<V: Fn(u64) -> u64, P: Fn(Index)>(&mut self, n: Index, values: V, progress: P) -> Result<()> {
    let file = self.file.as_mut().unwrap();
    let file_size = file.metadata()?.len();
    assert!(file_size % 8 == 0, "{file_size} is not a multiple of u64");
    let size = file_size / 8;
    assert!(size <= n);
    for i in size + 1..=n {
      file.write_all(&values(i).to_le_bytes())?;
      (progress)(i);
    }
    Ok(())
  }

  #[inline(never)]
  fn get<V: Fn(u64) -> u64>(&mut self, i: Index, values: V) -> Result<Duration> {
    let file = self.file.as_mut().unwrap();
    let file_size = file.seek(SeekFrom::End(0))?;
    assert!(file_size % 8 == 0);
    let mut buffer = vec![0u8; 8 * (1 << self.cache_level)];
    let mut position = file_size;
    let mut i_current = file_size / 8;
    let start = Instant::now();
    while position > 0 {
      let read_size = buffer.len().min(position as usize);
      position -= read_size as u64;
      file.seek(SeekFrom::Start(position))?;
      file.read_exact(&mut buffer[..read_size])?;
      for chunk in buffer[..read_size].rchunks_exact(8) {
        let value = u64::from_le_bytes(chunk.try_into().unwrap());
        if i_current == i {
          let elapse = start.elapsed();
          assert_eq!(values(i), value);
          return Ok(elapse);
        }
        i_current -= 1;
      }
    }
    panic!()
  }
}

impl AppendCUT for SeqFileCUT {
  #[inline(never)]
  fn append<V: Fn(u64) -> u64>(&mut self, n: Index, values: V) -> Result<(u64, Duration)> {
    let file = self.file.as_mut().unwrap();
    let file_size = file.metadata()?.len();
    let begin = file_size / 8;
    assert!(file_size % 8 == 0, "{file_size} is not a multiple of u64");
    assert!(begin <= n, "begin={begin} is larger than n={n}");
    file.seek(SeekFrom::End(0))?;
    let start = Instant::now();
    for i in (begin + 1)..=n {
      file.write_all(&values(i).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    let size = file.metadata()?.len();
    Ok((size, elapse))
  }

  fn clear(&mut self) -> Result<()> {
    let file = self.file.as_mut().unwrap();
    file.set_len(0)?;
    Ok(())
  }
}

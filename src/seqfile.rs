use slate::Result;
use slate_benchmark::splitmix64;
use std::fs::{File, OpenOptions, remove_file};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::{Case, Driver};

pub struct AppendDriver {
  path: Option<PathBuf>,
}
impl AppendDriver {
  pub fn new() -> Self {
    AppendDriver { path: None }
  }
}
impl Driver<File, Duration> for AppendDriver {
  fn setup(&mut self, case: &Case) -> Result<File> {
    let path = case.file("seqfile.db");
    self.path = Some(path.clone());
    Ok(OpenOptions::new().create_new(true).write(true).open(&path)?)
  }

  #[inline(never)]
  fn run(&mut self, _case: &Case, file: &mut File, n: u64) -> Result<Duration> {
    let start = Instant::now();
    for i in 1..=n {
      file.write_all(&splitmix64(i).to_le_bytes())?;
    }
    let elapse = start.elapsed();
    Ok(elapse)
  }

  fn cleanup(&mut self, _case: &Case, file: File) -> Result<()> {
    drop(file);
    if let Some(path) = &self.path {
      remove_file(path)?;
    }
    self.path = None;
    Ok(())
  }
}

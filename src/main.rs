use ::slate::{BlockStorage, Index, Result, Slate};
use chrono::Local;
use clap::Parser;
use slate_benchmark::file_size;
use std::fs::{create_dir_all, remove_dir_all};
use std::path::PathBuf;
use std::time::Instant;
use tempfile::Builder;

mod rocksdb;
mod seqfile;
mod slate;
mod stat;

#[derive(Parser)]
#[command(name = "slate-bench")]
#[command(about = "Benchmark file operations with configurable output directory")]
struct Args {
  /// Output directory for benchmark results and working temporary files
  #[arg(index = 1, default_value = ".")]
  dir: PathBuf,
}

fn main() -> Result<()> {
  let args = Args::parse();

  let id = Local::now().format("%Y%m%d%H%M%S").to_string();

  // 作業ディレクトリ作成
  let dir = args.dir;
  create_dir_all(&dir)?;
  println!("Working directory: {:?}", &dir);

  run_append_rocksdb(&dir, &id)?;
  run_append_seqfile(&dir, &id)?;
  run_append_slate(&dir, &id)?;

  Ok(())
}

const MAX_N: Index = 1024 * 1024;
const DIV: usize = 8;
const LOOP: usize = 10;

fn run_append_seqfile(dir: &PathBuf, id: &str) -> Result<()> {
  println!("[seqfile::append::series]");
  let mut tf = Builder::new().prefix(".tmp-seqfile-append").suffix(".db").tempfile_in(dir).unwrap();
  let mut report = stat::Report::new();
  for n in (0..=MAX_N).step_by((MAX_N / DIV as u64).try_into().unwrap()) {
    for _ in 0..LOOP {
      // setup
      tf.as_file().set_len(0)?;

      // run
      let t0 = Instant::now();
      seqfile::append(tf.as_file_mut(), n as u32).unwrap();
      let t1 = Instant::now();

      report.add(n, t1 - t0);
    }
    let size = tf.as_file().metadata()?.len();
    let s = report.single(n);
    println!("  n={n}: {s}; {size} bytes");
  }

  // write report
  let path = dir.join(format!("{id}-seqfile-append.csv"));
  report.save_to_csv(&path)?;
  println!("==> {}", path.to_string_lossy());
  Ok(())
}

fn run_append_slate(dir: &PathBuf, id: &str) -> Result<()> {
  println!("[slate::append::hash-tree]");
  let tf = Builder::new().prefix(".tmp-slate-append").suffix(".db").tempfile_in(dir).unwrap();
  let mut report = stat::Report::new();
  for n in (0..=MAX_N).step_by((MAX_N / DIV as u64).try_into().unwrap()) {
    for _ in 0..LOOP {
      // setup
      tf.as_file().set_len(0)?;
      let storage = BlockStorage::from_file(tf.path(), false)?;
      let mut slate = Slate::new(storage)?;

      // run
      let t0 = Instant::now();
      slate::append(&mut slate, n as u32).unwrap();
      let t1 = Instant::now();

      report.add(n, t1 - t0);
    }
    let size = tf.as_file().metadata()?.len();
    let s = report.single(n);
    println!("  n={n}: {s}; {size} bytes");
  }

  // write report
  let path = dir.join(format!("{id}-slate-append.csv"));
  report.save_to_csv(&path)?;
  println!("==> {}", path.to_string_lossy());
  Ok(())
}

fn run_append_rocksdb(dir: &PathBuf, id: &str) -> Result<()> {
  use ::rocksdb::{DB, Options};

  println!("[rocksdb::append::series]");
  let mut report = stat::Report::new();
  for n in (0..=MAX_N).step_by((MAX_N / DIV as u64).try_into().unwrap()) {
    let mut size = 0;
    for _ in 0..LOOP {
      // setup
      let tf = Builder::new().prefix(".tmp-rocksdb-append").tempdir_in(dir).unwrap();
      let mut opts = Options::default();
      opts.create_if_missing(true);
      remove_dir_all(tf.path())?;
      let db = DB::open(&opts, tf.path()).unwrap();

      // run
      let t0 = Instant::now();
      rocksdb::append(&db, n as u32).unwrap();
      let t1 = Instant::now();

      size = file_size(tf.path());
      report.add(n, t1 - t0);
    }
    let s = report.single(n);
    println!("  n={n}: {s}; {size} bytes");
  }

  // write report
  let path = dir.join(format!("{id}-locksdb-append.csv"));
  report.save_to_csv(&path)?;
  println!("==> {}", path.to_string_lossy());
  Ok(())
}

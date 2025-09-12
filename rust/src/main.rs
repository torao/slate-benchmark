use ::slate::error::Error;
use ::slate::formula::{entry_access_distance, entry_access_distance_limits};
use ::slate::{Index, Result};
use chrono::Local;
use clap::Parser;
use rand::seq::SliceRandom;
use rayon::iter::Either;
use rayon::prelude::*;
use slate_benchmark::{ZipfDistribution, file_count_and_size, file_size, splitmix64};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::{self, create_dir_all};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::stat::{Unit, XYReport};

mod binarytree;
mod seqfile;
mod slate;
mod stat;

#[derive(Parser)]
#[command(name = "slate-bench")]
#[command(author, version, about = "Slateベンチマークツール - ファイル操作のパフォーマンステストを実行します")]
struct Args {
  /// ベンチマークで使用するデータサイズ（エントリ数）
  #[arg(default_value_t = 256u64)]
  data_size: u64,

  /// ベンチマーク実行時の作業用一時ファイルを格納するディレクトリ
  #[arg(short, long, default_value_t = std::env::temp_dir().to_string_lossy().into_owned())]
  dir: String,

  /// ベンチマーク結果（CSVファイル）を出力するディレクトリ
  #[arg(short, long, default_value_t = {std::env::current_dir().unwrap().to_string_lossy().into_owned()})]
  output: String,

  /// ベンチマークセッションの識別子（ファイル名に使用されます）
  #[arg(short, long, default_value_t = Local::now().format("%Y%m%d%H%M%S").to_string())]
  session: String,

  /// 作業用ディレクトリをクリーンアップして終了
  #[arg(short, long, default_value_t = false)]
  clean: bool,

  /// ベンチマークの最大実行時間（秒）
  #[arg(short = 't', long, default_value_t = 600)]
  timeout: u64,
}

fn main() -> Result<()> {
  let args = Args::parse();

  // 作業ディレクトリ作成
  let root = PathBuf::from_str(&args.dir).unwrap();
  create_dir_all(&root)?;
  println!("Working directory: {:?}", &root);

  let experiment = Experiment::new(&args)?;

  if args.clean {
    experiment.clean()?;
    return Ok(());
  }

  #[cfg(true)]
  {
    experiment
      .case(&args)?
      .max_n(args.data_size)
      .max_trials(500)
      .measure_the_frequency_of_retrieval_against_positions_by_zipf::<slate::FileSlateCUT>(
        "biased-get-slate-file",
        0,
      )?;
  }

  #[cfg(false)]
  {
    experiment
      .case(&args)?
      .max_n(args.data_size)
      .division(100)
      .scale(Scale::WorstCase)
      .max_trials(500)
      .cv_threshold(0.5)
      .measure_the_retrieval_time_relative_to_the_position::<slate::MemSlateCUT>("get-slate-memkvs", 0)?
      .measure_the_retrieval_time_relative_to_the_position::<slate::FileSlateCUT>("get-slate-file", 0)?
      .measure_the_retrieval_time_relative_to_the_position::<slate::RocksDBSlateCUT>("get-slate-rocksdb", 0)?
      .scale(Scale::Log)
      .measure_the_retrieval_time_relative_to_the_position::<binarytree::FileBinaryTreeCUT>("get-hashtree-file", 1)?;
  }

  #[cfg(false)]
  {
    experiment
      .case(&args)?
      .max_n(args.data_size)
      .division(10)
      .min_trials(2)
      .max_trials(10)
      .measure_the_append_time_relative_to_the_data_amount::<slate::FileSlateCUT>("slate-file")?
      .measure_the_append_time_relative_to_the_data_amount::<slate::RocksDBSlateCUT>("slate-rocksdb")?
      .measure_the_append_time_relative_to_the_data_amount::<seqfile::SeqFileCUT>("seqfile-file")?
      .measure_the_append_time_relative_to_the_data_amount::<slate::MemSlateCUT>("slate-memory")?;
  }

  #[cfg(false)]
  {
    for level in 0..=3 {
      experiment
        .case(&args)?
        .max_n(args.data_size)
        .division(64)
        .scale(Scale::WorstCase)
        .max_trials(1000)
        .cv_threshold(0.5)
        .measure_the_retrieval_time_relative_to_the_position::<slate::FileSlateCUT>(
          &format!("cache-slate-file-{level}"),
          level,
        )?;
    }
  }

  #[cfg(false)]
  {
    experiment
      .case(&args)?
      .max_n(args.data_size)
      .scale(Scale::WorstCase)
      .measure_the_prove_time_relative_to_the_position::<slate::FileSlateCUT>("prove-slate-file")?
      .measure_the_prove_time_relative_to_the_position::<slate::RocksDBSlateCUT>("prove-slate-rocksdb")?;
  }

  Ok(())
}

pub enum Scale {
  Linear,
  Log,
  BestCase,
  WorstCase,
}

struct Experiment {
  session: String,
  dir: PathBuf,
  dir_report: PathBuf,

  stability_threshold: f64, // 例: 0.10 (=10%)
  min_trials: usize,        // 例: 5
  max_trials: usize,        // 例: 100
  max_duration: Duration,   // 例: Duration::from_secs(30),
}

pub struct Case {
  pub session: String,
  pub dir: PathBuf,
  pub dir_report: PathBuf,
  pub min_n: Index,
  pub max_n: Index,
  scale: Scale,
  division: usize,
  cv_threshold: f64,      // 例: 0.10 (=10%)
  min_trials: usize,      // 例: 5
  max_trials: usize,      // 例: 100
  max_duration: Duration, // 例: Duration::from_secs(30),
}

impl Experiment {
  fn new(args: &Args) -> Result<Self> {
    let session = args.session.clone();
    let dir = PathBuf::from(&args.dir);
    let dir_report = PathBuf::from(&args.output);

    if !dir.exists() {
      create_dir_all(&dir)?;
    }
    if !dir_report.exists() {
      create_dir_all(&dir)?;
    }

    let stability_threshold = 0.05;
    let min_trials = 5;
    let max_trials = 1000;
    let max_duration = Duration::from_secs(args.timeout);
    Ok(Self { session, dir, dir_report, stability_threshold, min_trials, max_trials, max_duration })
  }

  pub fn case(&self, args: &Args) -> Result<Case> {
    let session = self.session.clone();
    let dir = self.dir.clone();
    let dir_report = self.dir_report.clone();
    let min_n = 1;
    let max_n = args.data_size;
    let scale = Scale::Linear;
    let division = 100;

    let stability_threshold = self.stability_threshold;
    let min_trials = self.min_trials;
    let max_trials = self.max_trials;
    let max_duration = self.max_duration;
    Ok(Case {
      session,
      dir,
      dir_report,
      min_n,
      max_n,
      scale,
      division,
      cv_threshold: stability_threshold,
      min_trials,
      max_trials,
      max_duration,
    })
  }

  fn clean(&self) -> Result<()> {
    let mut total = 0u64;
    let mut count = 0;
    for entry in fs::read_dir(&self.dir)? {
      let e = entry?;
      if e.file_name().to_str().unwrap().starts_with("slate_benchmark-") {
        if e.file_type()?.is_dir() {
          let dir_path = e.path();
          let size = file_size(&dir_path);
          println!("Removing: {} ({} bytes)", dir_path.display(), size);
          fs::remove_dir_all(&dir_path)?;
          total += size;
        } else if e.file_type()?.is_file() {
        }
        count += 1;
      }
      if e.file_type()?.is_dir() && e.file_name().to_str().unwrap().starts_with("slate_benchmark-") {
        let dir_path = e.path();
        let size = file_size(&dir_path);
        println!("Removing: {} ({} bytes)", dir_path.display(), size);
        fs::remove_dir_all(&dir_path)?;
        total += size;
        count += 1;
      }
    }
    eprintln!("{count} files are removed, total {total} bytes");
    Ok(())
  }
}

macro_rules! property_decl {
  ($name:ident, $type:ident) => {
    pub fn $name(mut self, $name: $type) -> Self {
      self.$name = $name;
      self
    }
  };
}

impl Case {
  property_decl!(min_n, Index);
  property_decl!(max_n, Index);
  property_decl!(division, usize);
  property_decl!(scale, Scale);
  property_decl!(cv_threshold, f64);
  property_decl!(min_trials, usize);
  property_decl!(max_trials, usize);
  property_decl!(max_duration, Duration);

  pub fn file(&self, id: &str, filename: &str) -> PathBuf {
    self.dir_work(id).join(filename)
  }

  pub fn name(&self, id: &str) -> String {
    format!("{}-{id}", self.session)
  }

  pub fn dir_work(&self, id: &str) -> PathBuf {
    let dir_work = self.dir.join(format!("slate_benchmark-{}", self.name(id)));
    if !dir_work.exists() {
      create_dir_all(&dir_work).unwrap();
    }
    dir_work
  }

  fn gauge(&self) -> Vec<u64> {
    let gauge = match self.scale {
      Scale::Linear => linspace(self.min_n, self.max_n, self.division),
      Scale::Log => logspace(self.min_n, self.max_n, self.division),
      Scale::BestCase => {
        let (_, ll) = entry_access_distance_limits(self.max_n);
        ll.into_iter()
          .enumerate()
          .flat_map(|(d, range)| range.filter(move |k| entry_access_distance(*k, self.max_n).unwrap() == d as u8))
          .collect::<Vec<_>>()
      }
      Scale::WorstCase => {
        let (ul, _) = entry_access_distance_limits(self.max_n);
        ul.into_iter()
          .enumerate()
          .flat_map(|(d, range)| range.filter(move |k| entry_access_distance(*k, self.max_n).unwrap() == d as u8))
          .collect::<Vec<_>>()
      }
    };
    // remove duplicates
    let mut seen = HashSet::new();
    gauge.into_iter().filter(|x| seen.insert(*x)).collect::<Vec<_>>()
  }

  fn clean(&self, id: &str) -> Result<(usize, u64)> {
    let (count, size) = file_count_and_size(self.dir_work(id));
    println!("Clean: {}: removing {count} files ({size} bytes)", self.dir_work(id).to_string_lossy());
    fs::remove_dir_all(self.dir_work(id))?;
    Ok((count, size))
  }

  /// データ量に対する追記時間を計測します。
  pub fn measure_the_append_time_relative_to_the_data_amount<CUT>(self, id: &str) -> Result<Self>
  where
    CUT: AppendCUT,
  {
    println!("\n=== Append Benchmark ({id}) ===\n");
    println!("DataSize\tMean[ms]\tStdDev[ms]\tCV[%]\t\tTrials");
    println!("--------\t--------\t----------\t-----\t\t------");

    let mut space_complexity = stat::XYReport::new(stat::Unit::Bytes);
    let mut time_complexity = stat::XYReport::new(stat::Unit::Milliseconds);
    let gauge = self.gauge();
    let start = Instant::now();
    for trials in 0..self.max_trials {
      if trials > self.min_trials && filter_cv_sufficient(&gauge, &time_complexity, self.cv_threshold).is_empty() {
        let s = time_complexity.calculate(&self.max_n).unwrap();
        println!("{}\t\t{:.1}ms\t{:.2}ms\t\t{:.3}\t\t{}\n", self.max_n, s.mean, s.std_dev, s.cv(), trials);
        break;
      }
      if start.elapsed() > self.max_duration {
        println!("** TIMED OUT **");
        let s = time_complexity.calculate(&self.max_n).unwrap();
        println!("{}\t\t{:.1}ms\t{:.2}ms\t\t{:.3}\t\t{}\n", self.max_n, s.mean, s.std_dev, s.cv(), trials);
        break;
      }

      let mut cum_time = Duration::ZERO;
      let target = CUT::prepare(&self, id, 0, splitmix64)?;
      for n in gauge.iter() {
        let (size, time) = CUT::append(&target, *n)?;
        if trials == 0 {
          space_complexity.add(n, size);
        }
        cum_time += time;
        time_complexity.add(n, cum_time.as_nanos() as f64 / 1000.0 / 1000.0);
      }
      CUT::remove(&target)?;
      if trials % 100 == 99 {
        let s = time_complexity.calculate(&self.max_n).unwrap();
        println!("{}\t\t{:.1}ms\t{:.2}ms\t\t{:.3}\t\t{}\n", self.max_n, s.mean, s.std_dev, s.cv(), trials + 1);
      }
    }

    // write report
    let name = format!("{}-volume-{}", self.session, id);
    let path = self.dir_report.join(format!("{name}.csv"));
    space_complexity.save_xy_to_csv(&path, "SIZE", "BYTES")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    let name = format!("{}-append-{}", self.session, id);
    let path = self.dir_report.join(format!("{name}.csv"));
    time_complexity.save_xy_to_csv(&path, "SIZE", "MILLISECONDS")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }

  /// アクセス位置に対するデータ取得時間を計測します。
  pub fn measure_the_retrieval_time_relative_to_the_position<CUT>(self, id: &str, cache_size: usize) -> Result<Self>
  where
    CUT: GetCUT,
  {
    println!("\n=== Get Benchmark ({id}) ===\n");

    // データベースを作成
    println!("Creating database with {} entries...", self.max_n);
    let t0 = Instant::now();
    let target = CUT::prepare(&self, id, self.max_n, splitmix64)?;
    let t = t0.elapsed();
    println!("created: {:.3} [msec]", t.as_nanos() as f64 / 1000.0 / 1000.0);

    println!("DataSize\tCV[%]\t\tTrials");
    println!("--------\t--------\t----------");

    let mut time_complexity = stat::XYReport::new(stat::Unit::Milliseconds);
    let mut rng = rand::rng();
    let mut gauge = self.gauge();
    let start = Instant::now();
    for trials in 0..self.max_trials {
      if trials > self.min_trials {
        gauge = filter_cv_sufficient(&gauge, &time_complexity, self.cv_threshold);
        if gauge.is_empty() {
          println!("{}\t\t{:.3}\t\t{}/{}", self.max_n, time_complexity.max_cv(), trials + 1, self.max_trials);
          break;
        }
      }
      if start.elapsed() >= self.max_duration {
        println!("** TIMED OUT **");
        println!("{}\t\t{:.3}\t\t{}/{}", self.max_n, time_complexity.max_cv(), trials, self.max_trials);
        break;
      }

      gauge.shuffle(&mut rng);
      let results = CUT::gets(&target, &gauge, cache_size, splitmix64)?;
      for (i, duration) in results {
        time_complexity.add(&i, duration.as_nanos() as f64 / 1000.0 / 1000.0);
      }
      if (trials + 1) % 100 == 0 {
        println!("{}\t\t{:.3}\t\t{}/{}", self.max_n, time_complexity.max_cv(), trials + 1, self.max_trials);
      }
    }
    CUT::remove(&target)?;

    // write report
    let path = self.dir_report.join(format!("{}.csv", self.name(id)));
    time_complexity.save_xy_to_csv(&path, "DISTANCE", "ACCESS TIME")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }

  /// Zipf 分布に従うアクセス位置に対するデータ取得時間の頻度を計測します。
  pub fn measure_the_frequency_of_retrieval_against_positions_by_zipf<CUT>(
    self,
    id: &str,
    cache_size: usize,
  ) -> Result<Self>
  where
    CUT: GetCUT,
  {
    println!("\n=== Zipf Get Benchmark ({id}) ===\n");

    // データベースを作成
    println!("Creating database with {} entries...", self.max_n);
    let t0 = Instant::now();
    let target = CUT::prepare(&self, id, self.max_n, splitmix64)?;
    let t = t0.elapsed();
    println!("created: {:.3} [msec]", t.as_nanos() as f64 / 1000.0 / 1000.0);

    println!("Shape\t\tDataSize\tMean\t\tStdDev\t\tMax\t\tTrials");
    println!("--------\t--------\t--------\t--------\t--------\t----------");

    let mut position_frequency = XYReport::new(Unit::Bytes);
    let mut time_frequency = XYReport::new(Unit::Milliseconds);
    let mut batch = Vec::with_capacity(self.max_trials);
    for s in [0.5, 1.2, 1.5, 2.0] {
      let x_label = format!("{s:.1}");
      let mut dist = ZipfDistribution::new(100, s, self.max_n - 1);
      for _ in 0..self.max_trials {
        batch.truncate(0);
        while batch.len() < batch.capacity() {
          batch.push(dist.next_u64());
        }
      }

      let results = CUT::gets(&target, &batch, cache_size, splitmix64)?;
      time_frequency
        .append(&x_label, results.iter().map(|(_, d)| d.as_nanos() as f64 / 1000.0 / 1000.0).collect::<Vec<_>>());
      position_frequency.append(&x_label, batch.clone());
      let stat = time_frequency.calculate(&x_label).unwrap();
      println!(
        "{}\t\t{}\t\t{:.3}\t\t{:.3}\t\t{:.3}\t\t{}",
        x_label, self.max_n, stat.mean, stat.std_dev, stat.max, self.max_trials
      );
    }
    CUT::remove(&target)?;

    // write report
    let path = self.dir_report.join(format!("{}_x.csv", self.name(id)));
    position_frequency.save_xy_to_csv(&path, "ZIPF", "POSITION")?;
    let path = self.dir_report.join(format!("{}_y.csv", self.name(id)));
    time_frequency.save_xy_to_csv(&path, "ZIPF", "MILLISECONDS")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }

  // データ差異の位置に対する差分検出時間を計測します。
  fn measure_the_prove_time_relative_to_the_position<CUT>(self, id: &str) -> Result<Self>
  where
    CUT: ProveCUT,
  {
    println!("\n=== Prove Benchmark ({id}) ===\n");
    let mut gauge = self.gauge();

    print!("Creating {} files with one difference each: ", gauge.len());
    let (errs, targets): (Vec<Error>, Vec<_>) = gauge
      .par_iter()
      .cloned()
      .map(Some)
      .chain(vec![None])
      .map(|i| {
        let target = CUT::prepare(&self, id, self.max_n, |k| {
          let value = splitmix64(k);
          if i.map(|i| i == k).unwrap_or(false) { splitmix64(value) } else { value }
        })?;
        print!("*");
        Ok((i, target))
      })
      .partition_map(|target| match target {
        Ok(target) => Either::Right(target),
        Err(err) => Either::Left(err),
      });
    if !errs.is_empty() {
      println!(": preparation failure");
      return self.clean(id).map(|_| self);
    }
    let targets = targets.into_iter().collect::<HashMap<_, _>>();
    println!(": preparation completed");

    println!("DataSize\tCV[%]\t\tTrials");
    println!("--------\t--------\t----------");

    let mut rng = rand::rng();
    let mut time_complexity = stat::XYReport::new(stat::Unit::Milliseconds);
    let start = Instant::now();
    for trials in 0..self.max_trials {
      if trials > self.min_trials {
        gauge = filter_cv_sufficient(&gauge, &time_complexity, self.cv_threshold);
        if gauge.is_empty() {
          println!("{}\t\t{:.3}\t\t{}/{}", self.max_n, time_complexity.max_cv(), trials, self.max_trials);
          break;
        }
      }
      if start.elapsed() >= self.max_duration {
        println!("** TIMED OUT **");
        println!("{}\t\t{:.3}\t\t{}/{}", self.max_n, time_complexity.max_cv(), trials, self.max_trials);
        break;
      }

      gauge.shuffle(&mut rng);
      for i in gauge.iter().cloned() {
        let target1 = targets.get(&None).unwrap();
        let target2 = targets.get(&Some(i)).unwrap();
        let (result, elapse) = CUT::prove(target1, target2)?;
        assert_eq!(Some(i), result);
        time_complexity.add(&(self.max_n - i + 1), elapse.as_nanos() as f64 / 1000.0 / 1000.0);
      }
      if trials % 100 == 99 {
        println!("{}\t\t{:.3}\t\t{}/{}", self.max_n, time_complexity.max_cv(), trials + 1, self.max_trials);
      }
    }

    // write report
    let path = self.dir_report.join(format!("{}.csv", self.name(id)));
    time_complexity.save_xy_to_csv(&path, "DISTANCE", "DETECT TIME")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());

    return self.clean(id).map(|_| self);
  }
}

fn filter_cv_sufficient(gauge: &[u64], ss: &stat::XYReport<u64, f64>, cv: f64) -> Vec<u64> {
  gauge.iter().filter(|i| !ss.is_cv_sufficient(**i, cv)).cloned().collect::<Vec<_>>()
}

/// Component under Test.
pub trait CUT {
  type T: std::marker::Send + Debug;

  /// n 個のデータを投入したデータベースを作成する。
  fn prepare<T: Fn(Index) -> u64>(case: &Case, id: &str, n: Index, values: T) -> Result<Self::T>;

  /// 指定されたデータベースを削除する。
  fn remove(target: &Self::T) -> Result<()>;
}

pub trait AppendCUT: CUT {
  fn append(target: &Self::T, n: Index) -> Result<(u64, Duration)>;
}

pub trait GetCUT: CUT {
  fn gets<V: Fn(u64) -> u64>(
    target: &Self::T,
    is: &[Index],
    cache_size: usize,
    values: V,
  ) -> Result<Vec<(u64, Duration)>>;
}

pub trait ProveCUT: CUT + Sync {
  fn prove(target1: &Self::T, target2: &Self::T) -> Result<(Option<u64>, Duration)>;
}

pub trait IntoFloat: Copy {
  fn into_f64(self) -> f64;
}

impl IntoFloat for u64 {
  fn into_f64(self) -> f64 {
    self as f64
  }
}

impl IntoFloat for f64 {
  fn into_f64(self) -> f64 {
    self
  }
}

fn linspace(min: u64, max: u64, n: usize) -> Vec<u64> {
  assert!(n > 1);
  let step = (max - min) as f64 / (n - 1) as f64;
  (0..n)
    .map(|i| {
      let val = min as f64 + step * i as f64;
      val.round() as u64
    })
    .collect()
}

fn logspace(min: u64, max: u64, n: usize) -> Vec<u64> {
  assert!(min > 0, "min must be positive for logspace");
  assert!(n > 1);
  let log_min = (min as f64).ln();
  let log_max = (max as f64).ln();
  let step = (log_max - log_min) / (n - 1) as f64;
  (0..n)
    .map(|i| {
      let val = (log_min + step * i as f64).exp();
      val.round() as u64
    })
    .collect()
}

use ::slate::formula::{entry_access_distance, entry_access_distance_limits};
use ::slate::{Index, Result};
use chrono::Local;
use clap::Parser;
use rand::seq::SliceRandom;
use slate_benchmark::file_size;
use std::cmp;
use std::fs::{self, create_dir_all};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::stat::Stat;

mod binarytree;
mod seqfile;
mod slate;
mod stat;

#[derive(Parser)]
#[command(name = "slate-bench")]
#[command(author, version, about = "Benchmark file operations with configurable output directory")]
struct Args {
  #[arg(default_value_t = 256u64)]
  data_size: u64,

  /// Output directory for benchmark results and working temporary files
  #[arg(short, long, default_value_t = std::env::temp_dir().to_string_lossy().into_owned())]
  dir: String,

  #[arg(short, long, default_value_t = {std::env::current_dir().unwrap().to_string_lossy().into_owned()})]
  output: String,

  #[arg(short, long, default_value_t = Local::now().format("%Y%m%d%H%M%S").to_string())]
  session: String,

  #[arg(short, long, default_value_t = false)]
  clean: bool,
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
      .case("volume-slate-file", &args, false)?
      .min_n(0)
      .max_n(args.data_size)
      .division(25)
      .min_trials(1)
      .max_trials(1)
      .measure_the_storage_size_relative_to_the_amount_of_data(slate::FileVolumeDriver::new())?;

    experiment
      .case("volume-slate-rocksdb", &args, false)?
      .min_n(0)
      .max_n(args.data_size)
      .division(25)
      .min_trials(1)
      .max_trials(1)
      .measure_the_storage_size_relative_to_the_amount_of_data(slate::RocksDBVolumeDriver::new())?;
  }

  #[cfg(false)]
  experiment
    .case("query-slate-file-large", &args, true)?
    .min_n(1)
    .max_n(args.data_size)
    .division(500)
    .scale(Scale::Log)
    .max_trials(100)
    .stability_threshold(0.5)
    .measure_the_data_retrieval_time_relative_to_the_access_location(slate::FileQueryDriver::new())?;

  #[cfg(true)]
  {
    experiment
      .case("query-slate-memkvs", &args, false)?
      .max_n(args.data_size)
      .division(100)
      .scale(Scale::WorstCase)
      .max_trials(500)
      .stability_threshold(0.5)
      .measure_the_data_retrieval_time_relative_to_the_access_location(slate::MemKVSQueryDriver::new())?;

    experiment
      .case("query-slate-file", &args, false)?
      .max_n(args.data_size)
      .division(100)
      .scale(Scale::WorstCase)
      .max_trials(500)
      .stability_threshold(0.5)
      .measure_the_data_retrieval_time_relative_to_the_access_location(slate::FileQueryDriver::new())?;

    experiment
      .case("query-slate-rocksdb", &args, false)?
      .max_n(1024 * 1024)
      .division(100)
      .scale(Scale::WorstCase)
      .max_trials(500)
      .stability_threshold(0.5)
      .measure_the_data_retrieval_time_relative_to_the_access_location(slate::RocksDBQueryDriver::new())?;

    experiment
      .case("query-hashtree-file", &args, false)?
      .max_n(1024 * 1024)
      .division(100)
      .scale(Scale::Log)
      .min_trials(5)
      .max_trials(500)
      .stability_threshold(0.5)
      .measure_the_data_retrieval_time_relative_to_the_access_location(binarytree::BinTreeQueryDiver::new())?;
  }

  #[cfg(true)]
  {
    experiment
      .case("append-seqfile-file", &args, false)?
      .max_n(args.data_size)
      .division(10)
      .min_trials(2)
      .max_trials(10)
      .measure_the_append_time_relative_to_the_amount_of_data(seqfile::AppendDriver::new())?;

    experiment
      .case("append-slate-memory", &args, false)?
      .max_n(args.data_size)
      .division(10)
      .min_trials(2)
      .max_trials(10)
      .measure_the_append_time_relative_to_the_amount_of_data(slate::MemoryAppendDriver::new())?;

    experiment
      .case("append-slate-file", &args, false)?
      .max_n(args.data_size)
      .division(10)
      .min_trials(2)
      .max_trials(10)
      .measure_the_append_time_relative_to_the_amount_of_data(slate::FileAppendDriver::new())?;

    experiment
      .case("append-slate-rocksdb", &args, false)?
      .max_n(args.data_size)
      .division(10)
      .min_trials(2)
      .max_trials(10)
      .measure_the_append_time_relative_to_the_amount_of_data(slate::RocksDBAppendDriver::new())?;
  }

  #[cfg(true)]
  {
    for level in 0..=3 {
      experiment
        .case(&format!("cache-slate-file-{level}"), &args, true)?
        .max_n(args.data_size)
        .division(64)
        .scale(Scale::WorstCase)
        .max_trials(1000)
        .stability_threshold(0.5)
        .measure_the_data_retrieval_time_relative_to_the_access_location(slate::FileCacheDriver::new(level))?;
    }
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
  pub id: String,
  pub name: String,
  pub dir_work: PathBuf,
  pub dir_report: PathBuf,
  pub min_n: Index,
  pub max_n: Index,
  scale: Scale,
  division: usize,
  persistent: bool,
  stability_threshold: f64, // 例: 0.10 (=10%)
  min_trials: usize,        // 例: 5
  max_trials: usize,        // 例: 100
  max_duration: Duration,   // 例: Duration::from_secs(30),
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

    let stability_threshold = 0.10;
    let min_trials = 5;
    let max_trials = 100;
    let max_duration = Duration::from_secs(10 * 60);
    Ok(Self { session, dir, dir_report, stability_threshold, min_trials, max_trials, max_duration })
  }

  pub fn case(&self, id: &str, args: &Args, persistent: bool) -> Result<Case> {
    let id = id.to_string();
    let name = format!("{}-{id}", self.session);
    let dir_work = self.dir.join(format!("slate_banchmark-{}", if persistent { "persistent" } else { &name }));
    let dir_report = self.dir_report.clone();
    let min_n = 1;
    let max_n = args.data_size;
    let scale = Scale::Linear;
    let division = 100;

    if !dir_work.exists() {
      create_dir_all(&dir_work)?;
    }

    let stability_threshold = self.stability_threshold;
    let min_trials = self.min_trials;
    let max_trials = self.max_trials;
    let max_duration = self.max_duration;
    Ok(Case {
      id,
      name,
      dir_work,
      dir_report,
      persistent,
      min_n,
      max_n,
      scale,
      division,
      stability_threshold,
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
      if e.file_type()?.is_dir() && e.file_name().to_str().unwrap().starts_with("slate_banchmark-") {
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
  property_decl!(stability_threshold, f64);
  property_decl!(min_trials, usize);
  property_decl!(max_trials, usize);
  property_decl!(max_duration, Duration);

  pub fn file(&self, filename: &str) -> PathBuf {
    self.dir_work.join(filename)
  }

  fn gauge(&self) -> Vec<u64> {
    match self.scale {
      Scale::Linear => linspace(self.min_n, self.max_n, self.division),
      Scale::Log => logspace(self.min_n, self.max_n, self.division),
      Scale::BestCase => {
        let (ll, _) = entry_access_distance_limits(self.max_n);
        ll.into_iter()
          .enumerate()
          .flat_map(|(d, range)| range.filter(move |k| entry_access_distance(*k, self.max_n).unwrap() == d as u8))
          .collect::<Vec<_>>()
      }
      Scale::WorstCase => {
        let (_, ul) = entry_access_distance_limits(self.max_n);
        ul.into_iter()
          .enumerate()
          .flat_map(|(d, range)| range.filter(move |k| entry_access_distance(*k, self.max_n).unwrap() == d as u8))
          .collect::<Vec<_>>()
      }
    }
  }

  /// データ量に対する追記時間を計測します。
  pub fn measure_the_append_time_relative_to_the_amount_of_data<T, D>(&self, mut driver: D) -> Result<()>
  where
    D: Driver<T, Duration>,
  {
    println!("[{}]", self.id);
    let mut report = stat::Report::new(stat::Unit::Milliseconds);
    let gauge = self.gauge();
    for (i, n) in gauge.iter().enumerate() {
      self.prepare()?;
      eprint!("  [{}/{}] n={n}: ", i + 1, self.division);
      let mut size = 0;
      let results = self.measure_until_stable(&mut || {
        let mut target = driver.setup(self)?;
        let elapse = driver.run(self, &mut target, *n)?;
        size = cmp::max(size, data_size(&self.dir_work));
        driver.cleanup(self, target)?;
        Ok(elapse.as_nanos() as f64 / 1000.0 / 1000.0)
      })?;
      let stat = report.append(n, results);
      eprintln!("{stat}; {size} bytes");
      self.cleanup()?;
    }

    // write report
    let path = self.dir_report.join(format!("{}.csv", self.name));
    report.save_xy_to_csv(&path, "N", "TIME")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(())
  }

  /// アクセス位置に対するデータ取得時間を計測します。
  fn measure_the_data_retrieval_time_relative_to_the_access_location<T, D>(&self, mut driver: D) -> Result<()>
  where
    D: Driver<T, Duration>,
  {
    println!("[{}]", self.id);
    let mut target = driver.setup(self)?;

    let mut rng = rand::rng();
    let mut report = stat::Report::new(stat::Unit::Milliseconds);
    let mut gauge = self.gauge().iter().map(|i| self.max_n - i + 1).collect::<Vec<_>>();
    for count in 0..self.max_trials {
      if count >= cmp::max(2, self.min_trials) {
        let relative = report.max_relative();
        if !relative.is_nan() && relative <= self.stability_threshold {
          break;
        }
      }

      gauge.shuffle(&mut rng);
      for i in gauge.iter().cloned() {
        let elapse = driver.run(self, &mut target, i)?;
        report.add(self.max_n - i + 1, elapse.as_nanos() as f64 / 1000.0 / 1000.0);
      }
      if count % 100 == 99 {
        println!("  [{}/{}] n={}: {:.3}", count + 1, self.max_trials, self.max_n, report.max_relative());
      }
    }
    driver.cleanup(self, target)?;

    // write report
    let path = self.dir_report.join(format!("{}.csv", self.name));
    report.save_xy_to_csv(&path, "DISTANCE", "ACCESS TIME")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(())
  }

  // データ量に対するストレージ容量を計測します。
  fn measure_the_storage_size_relative_to_the_amount_of_data<T, D>(&self, mut driver: D) -> Result<()>
  where
    D: Driver<T, u64>,
  {
    println!("[{}]", self.id);
    let mut report = stat::Report::new(stat::Unit::Bytes);
    let mut target = driver.setup(self)?;
    let gauge = self.gauge();
    for (i, n) in gauge.iter().enumerate() {
      eprint!("  [{}/{}] n={n}: ", i + 1, self.division);
      let size = driver.run(self, &mut target, *n)?;
      let stat = report.add(n, size);
      eprintln!("{stat}");
    }
    driver.cleanup(self, target)?;

    // write report
    let path = self.dir_report.join(format!("{}.csv", self.name));
    report.save_xy_to_csv(&path, "N", "SIZE")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(())
  }

  /// 関数 `f` によって計測された値が所定の安定指針を満たすまで繰り返します。
  ///
  /// ## Parameters
  ///
  /// - `stability_threshold` - 計測値の ±2σ (信頼区間として約 95.45%) が平均の何 % に収める必要があるか。
  /// - `min_trials` - 最低試行回数
  /// - `max_trials` - 最大試行回数
  /// - `max_duration` - 計測時間限界
  ///
  fn measure_until_stable<F, T>(&self, f: &mut F) -> Result<Vec<T>>
  where
    F: FnMut() -> Result<T>,
    T: Copy + IntoFloat,
  {
    let mut results = Vec::new();
    let start = Instant::now();
    while results.len() < self.min_trials
      || (results.len() < self.max_trials
        && start.elapsed() < self.max_duration
        && Stat::from_vec(stat::Unit::Bytes /* 何でもよい */, &results).relative() > self.stability_threshold)
    {
      results.push(f()?);
    }
    Ok(results)
  }

  fn prepare(&self) -> Result<()> {
    if !self.persistent {
      if self.dir_work.exists() {
        fs::remove_dir_all(&self.dir_work)?;
      }
      fs::create_dir_all(&self.dir_work)?;
    }
    Ok(())
  }
  fn cleanup(&self) -> Result<()> {
    if !self.persistent {
      fs::remove_dir_all(&self.dir_work)?;
    }
    Ok(())
  }
}

pub trait Driver<T, V> {
  fn setup(&mut self, case: &Case) -> Result<T>;
  fn run(&mut self, case: &Case, db: &mut T, param: u64) -> Result<V>;
  fn cleanup(&mut self, _case: &Case, _db: T) -> Result<()> {
    Ok(())
  }
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

fn data_size(path: &Path) -> u64 {
  if path.is_dir() {
    fs::read_dir(path).unwrap().map(|f| data_size(&f.unwrap().path())).sum::<u64>()
  } else if path.is_file() {
    path.metadata().unwrap().len()
  } else {
    0
  }
}

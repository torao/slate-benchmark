use ::slate::error::Error;
use ::slate::formula::{entry_access_distance, entry_access_distance_limits};
use ::slate::{Index, Result};
use chrono::Local;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::seq::SliceRandom;
use rayon::iter::Either;
use rayon::{ThreadPoolBuilder, prelude::*};
use slate_benchmark::{ZipfDistribution, file_size, splitmix64};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Write, stdout};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::binarytree::FileBinaryTreeCUT;
use crate::seqfile::SeqFileCUT;
use crate::slate::{FileFactory, MemKVSFactory, RocksDBFactory, SlateCUT};
use crate::stat::{ExpirationTimer, Unit, XYReport};

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
  fs::create_dir_all(&root)?;
  println!("Working directory: {:?}", &root);

  let experiment = Experiment::new(&args)?;

  if args.clean {
    experiment.clean_all_experiments()?;
    return Ok(());
  }

  let dir = experiment.work_dir()?;
  {
    let mut cut = SlateCUT::new(MemKVSFactory::new(args.data_size as usize))?;
    experiment
      .run_testunit_append(&mut cut)?
      .run_testunit_biased_get(&mut cut)?
      .run_testunit_uniformed_get(&mut cut)?
      .run_testunit_cache_level(&mut cut)?
      .clear()?;
  }

  {
    let mut cut = SlateCUT::new(FileFactory::new(&dir))?;
    experiment
      .run_testunit_append(&mut cut)?
      .run_testunit_biased_get(&mut cut)?
      .run_testunit_uniformed_get(&mut cut)?
      .run_testunit_cache_level(&mut cut)?
      .run_testunit_prove(&mut cut)?
      .clear()?;
  }

  {
    let mut cut = SlateCUT::new(RocksDBFactory::new(&dir))?;
    experiment
      .run_testunit_append(&mut cut)?
      .run_testunit_biased_get(&mut cut)?
      .run_testunit_uniformed_get(&mut cut)?
      .run_testunit_cache_level(&mut cut)?
      .clear()?;
  }

  {
    let mut cut = SeqFileCUT::new(&dir)?;
    experiment
      .run_testunit_append(&mut cut)?
      .run_testunit_biased_get(&mut cut)?
      .run_testunit_uniformed_get(&mut cut)?
      .run_testunit_cache_level(&mut cut)?
      .clear()?;
  }

  {
    let mut cut = FileBinaryTreeCUT::new(&dir, args.data_size)?;
    experiment
      .run_testunit_biased_get(&mut cut)?
      .run_testunit_uniformed_get(&mut cut)?
      .run_testunit_cache_level(&mut cut)?
      .clear()?;
  }

  fs::remove_dir_all(&dir)?;
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
  data_size: Index,
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
      fs::create_dir_all(&dir)?;
    }
    if !dir_report.exists() {
      fs::create_dir_all(&dir)?;
    }

    let stability_threshold = 0.05;
    let min_trials = 5;
    let max_trials = 1000;
    let max_duration = Duration::from_secs(args.timeout);
    let data_size = args.data_size;
    Ok(Self { session, dir, dir_report, stability_threshold, min_trials, max_trials, max_duration, data_size })
  }

  pub fn case(&self) -> Result<Case> {
    let session = self.session.clone();
    let dir = self.dir.clone();
    let dir_report = self.dir_report.clone();
    let min_n = 1;
    let max_n = self.data_size;
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

  fn work_dir(&self) -> Result<PathBuf> {
    let path = self.dir.join(format!("slate_benchmark-{}", self.session));
    if !path.exists() {
      fs::create_dir_all(&path)?;
    }
    Ok(path)
  }

  fn clear(&self) -> Result<()> {
    let work_dir = self.work_dir()?;
    if work_dir.exists() {
      for entry in fs::read_dir(&work_dir)? {
        let e = entry?;
        let path = e.path();
        if e.file_type()?.is_dir() {
          fs::remove_dir_all(e.path()).unwrap();
          println!("directory removed: {}", path.to_string_lossy());
        } else if e.file_type()?.is_file() {
          fs::remove_file(e.path()).unwrap();
          println!("file removed: {}", path.to_string_lossy());
        } else {
          println!("WARN: unrecognized file type: {}", path.to_string_lossy());
        }
      }
    } else {
      fs::create_dir_all(&work_dir)?;
    }
    Ok(())
  }

  fn clean_all_experiments(&self) -> Result<()> {
    let mut total = 0u64;
    let mut count = 0;
    if self.dir.exists() {
      for entry in fs::read_dir(&self.dir)? {
        let e = entry?;
        if e.file_name().to_str().unwrap().starts_with("slate_benchmark-") {
          let path = e.path();
          let size = file_size(&path);
          println!("Removing: {} ({} bytes)", path.display(), size);
          if e.file_type()?.is_dir() {
            fs::remove_dir_all(&path)?;
          } else if e.file_type()?.is_file() {
            fs::remove_file(&path)?;
          }
          total += size;
          count += 1;
        }
      }
    }
    eprintln!("{count} files are removed, total {total} bytes");
    Ok(())
  }

  fn run_testunit_append<C: AppendCUT>(&self, cut: &mut C) -> Result<&Experiment> {
    self.case()?.division(10).min_trials(2).max_trials(10).measure_the_append_time_relative_to_the_data_amount(cut)?;
    Ok(self)
  }

  fn run_testunit_biased_get<C: GetCUT>(&self, cut: &mut C) -> Result<&Experiment> {
    self.case()?.max_trials(500).measure_the_frequency_of_retrieval_against_positions_by_zipf(cut)?;
    Ok(self)
  }

  fn run_testunit_uniformed_get<C: GetCUT>(&self, cut: &mut C) -> Result<&Experiment> {
    self
      .case()?
      .division(100)
      .scale(Scale::WorstCase)
      .max_trials(500)
      .measure_the_retrieval_time_relative_to_the_position(cut, "get", 0)?;
    Ok(self)
  }

  fn run_testunit_cache_level<C: GetCUT>(&self, cut: &mut C) -> Result<&Experiment> {
    for level in 0..=3 {
      self
        .case()?
        .division(64)
        .scale(Scale::WorstCase)
        .max_trials(1000)
        .measure_the_retrieval_time_relative_to_the_position(cut, &format!("cache{level}"), level)?;
    }
    Ok(self)
  }

  fn run_testunit_prove<C: ProveCUT>(&self, cut: &mut C) -> Result<&Experiment> {
    self.case()?.scale(Scale::WorstCase).measure_the_prove_time_relative_to_the_position(cut)?;
    Ok(self)
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
      fs::create_dir_all(&dir_work).unwrap();
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

  /// データ量に対する追記時間を計測します。
  pub fn measure_the_append_time_relative_to_the_data_amount<CUT>(self, cut: &mut CUT) -> Result<Self>
  where
    CUT: AppendCUT,
  {
    println!("\n{}", Local::now().format("%Y-%m-%d %H:%M:%S %Z"));
    println!("=== Append Benchmark ({}) ===\n", cut.implementation());

    let mut timer = ExpirationTimer::new(self.max_duration, 10, self.max_trials, 10);
    ExpirationTimer::heading_ms();

    let mut space_complexity = stat::XYReport::new(stat::Unit::Bytes);
    let mut time_complexity = stat::XYReport::new(stat::Unit::Milliseconds);
    let gauge = self.gauge();
    for trials in 0..self.max_trials {
      cut.clear()?;
      let mut cum_time = Duration::ZERO;
      for n in gauge.iter() {
        let (size, time) = cut.append(*n, splitmix64)?;
        if trials == 0 {
          space_complexity.add(n, size);
        }
        cum_time += time;
        time_complexity.add(n, cum_time.as_nanos() as f64 / 1000.0 / 1000.0);
      }

      if trials + 1 >= self.min_trials && filter_cv_sufficient(&gauge, &time_complexity, self.cv_threshold).is_empty() {
        let s = time_complexity.calculate(&self.max_n).unwrap();
        timer.summary_ms(self.max_n, s.mean, s.std_dev);
        break;
      }
      if timer.expired() {
        let s = time_complexity.calculate(&self.max_n).unwrap();
        timer.summary_ms(self.max_n, s.mean, s.std_dev);
        println!("** TIMED OUT **");
        break;
      }
      if timer.carried_out(1) {
        let s = time_complexity.calculate(&self.max_n).unwrap();
        timer.summary_ms(self.max_n, s.mean, s.std_dev);
      }
    }

    // write report
    let name = format!("{}-volume-{}", self.session, cut.implementation());
    let path = self.dir_report.join(format!("{name}.csv"));
    space_complexity.save_xy_to_csv(&path, "SIZE", "BYTES")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    let name = format!("{}-append-{}", self.session, cut.implementation());
    let path = self.dir_report.join(format!("{name}.csv"));
    time_complexity.save_xy_to_csv(&path, "SIZE", "MILLISECONDS")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }

  /// アクセス位置に対するデータ取得時間を計測します。
  pub fn measure_the_retrieval_time_relative_to_the_position<CUT>(
    self,
    cut: &mut CUT,
    action_id: &str,
    cache_level: usize,
  ) -> Result<Self>
  where
    CUT: GetCUT,
  {
    println!("\n{}", Local::now().format("%Y-%m-%d %H:%M:%S %Z"));
    println!("=== Get Benchmark ({}) ===", cut.implementation());

    // データベースを作成
    let start = Instant::now();
    print!("Preparing database with {} entries: ", self.max_n);
    stdout().flush().unwrap();
    cut.prepare(self.max_n, splitmix64)?;
    cut.set_cache_level(cache_level)?;
    println!("{}ms: done\n", start.elapsed().as_millis());

    let mut timer = ExpirationTimer::new(self.max_duration, 10, self.max_trials, 10);
    ExpirationTimer::heading_max_cv();

    let mut time_complexity = stat::XYReport::new(stat::Unit::Milliseconds);
    let mut rng = rand::rng();
    let mut gauge = self.gauge();
    'trials: for trials in 0..self.max_trials {
      gauge.shuffle(&mut rng);
      for i in gauge.iter() {
        let duration = cut.get(*i, splitmix64)?;
        time_complexity.add(i, duration.as_nanos() as f64 / 1000.0 / 1000.0);

        if timer.expired() {
          timer.summary_max_cv(self.max_n, time_complexity.max_cv());
          println!("** TIMED OUT **");
          break 'trials;
        }
      }

      if trials + 1 >= self.min_trials {
        gauge = filter_cv_sufficient(&gauge, &time_complexity, self.cv_threshold);
        if gauge.is_empty() {
          timer.summary_max_cv(self.max_n, time_complexity.max_cv());
          break;
        }
      }
      if timer.carried_out(1) {
        timer.summary_max_cv(self.max_n, time_complexity.max_cv());
      }
    }

    // write report
    let id = format!("{action_id}-{}", cut.implementation());
    let path = self.dir_report.join(format!("{}.csv", self.name(&id)));
    time_complexity.save_xy_to_csv(&path, "DISTANCE", "ACCESS TIME")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }

  /// Zipf 分布に従うアクセス位置に対するデータ取得時間の頻度を計測します。
  pub fn measure_the_frequency_of_retrieval_against_positions_by_zipf<CUT>(self, cut: &mut CUT) -> Result<Self>
  where
    CUT: GetCUT,
  {
    println!("\n{}", Local::now().format("%Y-%m-%d %H:%M:%S %Z"));
    println!("=== Zipf Get Benchmark ({}) ===", cut.implementation());

    // データベースを作成
    let start = Instant::now();
    print!("Preparing database with {} entries: ", self.max_n);
    stdout().flush().unwrap();
    cut.prepare(self.max_n, splitmix64)?;
    cut.set_cache_level(0)?;
    println!("{}ms: done", start.elapsed().as_millis());

    let mut position_frequency = XYReport::new(Unit::Bytes);
    let mut time_frequency = XYReport::new(Unit::Milliseconds);
    for s in [0.5, 1.2, 1.5, 2.0] {
      let x_label = format!("{s:.1}");
      println!("\nShape = {x_label}");
      let mut timer = ExpirationTimer::new(self.max_duration, 10, self.max_trials, 10);
      ExpirationTimer::heading_ms();

      let mut dist = ZipfDistribution::new(100, s, self.max_n - 1);
      for _ in 0..self.max_trials {
        let position = dist.next_u64();
        let d = cut.get(position, splitmix64)?;
        time_frequency.add(&x_label, d.as_nanos() as f64 / 1000.0 / 1000.0);
        position_frequency.add(&x_label, position);

        if timer.expired() {
          let s = time_frequency.calculate(&x_label).unwrap();
          timer.summary_ms(self.max_n, s.mean, s.std_dev);
          println!("** TIMED OUT **");
          break;
        }
        if timer.carried_out(1) {
          let s = time_frequency.calculate(&x_label).unwrap();
          timer.summary_ms(self.max_n, s.mean, s.std_dev);
        }
      }
    }

    // write report
    let id = format!("biased-get-{}", cut.implementation());
    let path = self.dir_report.join(format!("{}_x.csv", self.name(&id)));
    position_frequency.save_xy_to_csv(&path, "ZIPF", "POSITION")?;
    let path = self.dir_report.join(format!("{}_y.csv", self.name(&id)));
    time_frequency.save_xy_to_csv(&path, "ZIPF", "MILLISECONDS")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }

  // データ差異の位置に対する差分検出時間を計測します。
  fn measure_the_prove_time_relative_to_the_position<CUT>(self, cut: &mut CUT) -> Result<Self>
  where
    CUT: ProveCUT,
  {
    println!("\n{}", Local::now().format("%Y-%m-%d %H:%M:%S %Z"));
    println!("=== Prove Benchmark ({}) ===", cut.implementation());
    let mut gauge = self.gauge();

    // プログレスバーの準備
    let pb = ProgressBar::new(gauge.len() as u64 * self.max_n);
    pb.set_style(
      ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .map_err(|e| ::slate::error::Error::Otherwise { source: e.into() })?
        .progress_chars("#>-"),
    );

    println!("Preparing {} files with one difference each...", gauge.len());
    let thread_pool = ThreadPoolBuilder::new().num_threads(25).build().unwrap();
    cut.prepare(self.max_n, splitmix64)?;
    let (mut errs, targets): (Vec<Error>, Vec<_>) = thread_pool.install(|| {
      gauge
        .iter()
        .copied()
        .map(|i| (i, cut.alternate()))
        .par_bridge()
        .map(|(i, alt)| match alt {
          Ok(mut alt) => {
            alt.prepare(self.max_n, |k| {
              pb.inc(1);
              let value = splitmix64(k);
              if i == k { splitmix64(value) } else { value }
            })?;
            stdout().flush().unwrap();
            Ok((i, alt))
          }
          Err(err) => Err(err),
        })
        .partition_map(|target| match target {
          Ok(target) => Either::Right(target),
          Err(err) => Either::Left(err),
        })
    });
    pb.finish();
    if !errs.is_empty() {
      drop(targets);
      for err in errs.iter() {
        eprintln!("ERROR: {err:?}");
      }
      return Err(errs.pop().unwrap());
    }
    let cuts = targets.into_iter().collect::<HashMap<_, _>>();
    println!("preparation completed\n");

    let mut timer = ExpirationTimer::new(self.max_duration, 10, self.max_trials, 10);
    ExpirationTimer::heading_max_cv();

    let mut rng = rand::rng();
    let mut time_complexity = stat::XYReport::new(stat::Unit::Milliseconds);
    for trials in 0..self.max_trials {
      gauge.shuffle(&mut rng);
      for i in gauge.iter().cloned() {
        let other = cuts.get(&i).unwrap();
        let (result, elapse) = cut.prove(other)?;
        assert_eq!(Some(i), result);
        time_complexity.add(&(self.max_n - i + 1), elapse.as_nanos() as f64 / 1000.0 / 1000.0);
      }

      if trials + 1 >= self.min_trials {
        gauge = filter_cv_sufficient(&gauge, &time_complexity, self.cv_threshold);
        if gauge.is_empty() {
          timer.summary_max_cv(self.max_n, time_complexity.max_cv());
          break;
        }
      }
      if timer.expired() {
        timer.summary_max_cv(self.max_n, time_complexity.max_cv());
        println!("** TIMED OUT **");
        break;
      }
      if timer.carried_out(1) {
        timer.summary_max_cv(self.max_n, time_complexity.max_cv());
      }
    }

    // write report
    let id = format!("prove-{}", cut.implementation());
    let path = self.dir_report.join(format!("{}.csv", self.name(&id)));
    time_complexity.save_xy_to_csv(&path, "DISTANCE", "DETECT TIME")?;
    println!("==> The results have been saved in: {}", path.to_string_lossy());
    Ok(self)
  }
}

fn filter_cv_sufficient(gauge: &[u64], ss: &stat::XYReport<u64, f64>, cv: f64) -> Vec<u64> {
  gauge.iter().filter(|i| !ss.is_cv_sufficient(**i, cv)).cloned().collect::<Vec<_>>()
}

// Component under Test.

pub trait CUT {
  fn implementation(&self) -> String;
}

pub trait GetCUT: CUT {
  fn set_cache_level(&mut self, cache_size: usize) -> Result<()>;
  fn prepare<V: Fn(u64) -> u64>(&mut self, n: Index, values: V) -> Result<()>;
  fn get<V: Fn(u64) -> u64>(&mut self, i: Index, values: V) -> Result<Duration>;
}

pub trait AppendCUT: CUT {
  /// ## Returns
  /// - (storage size, duration)
  fn append<V: Fn(u64) -> u64>(&mut self, n: Index, values: V) -> Result<(u64, Duration)>;
  fn clear(&mut self) -> Result<()>;
}

pub trait ProveCUT: GetCUT + Sync + Send {
  fn prove(&self, other: &Self) -> Result<(Option<u64>, Duration)>;
  fn alternate(&self) -> Result<Self>
  where
    Self: std::marker::Sized;
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

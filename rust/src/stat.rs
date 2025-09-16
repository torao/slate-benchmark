use crate::IntoFloat;
use chrono::{DateTime, Local};
use core::f64;
use slate::Result;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Clone)]
pub struct Stat {
  unit: Unit,
  pub count: usize,
  pub mean: f64,
  pub median: f64,
  pub std_dev: f64,
  pub min: f64,
  pub max: f64,
}

impl Stat {
  /// calculate StdDev / Mean
  pub fn cv(&self) -> f64 {
    self.std_dev / self.mean
  }

  pub fn from_vec<T: IntoFloat>(unit: Unit, data: &[T]) -> Stat {
    if data.is_empty() {
      return Stat {
        unit,
        count: 0,
        mean: f64::NAN,
        median: f64::NAN,
        std_dev: f64::NAN,
        min: f64::NAN,
        max: f64::NAN,
      };
    }
    let mut data = data.iter().map(|y| y.into_f64()).collect::<Vec<_>>();
    let count = data.len();
    let min = *data.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let max = *data.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let sum = data.iter().map(|y| y.into_f64()).sum::<f64>();
    let mean = sum / count as f64;
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = if count % 2 == 0 {
      let mid = count / 2;
      (data[mid - 1] + data[mid]) / 2.0
    } else {
      data[count / 2]
    };
    let variance = data
      .iter()
      .map(|&x| {
        let diff = x - mean;
        diff * diff
      })
      .sum::<f64>()
      / count as f64;
    let std_dev = variance.sqrt();
    Stat { unit, count, mean, median, std_dev, min, max }
  }
}

impl Display for Stat {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    // 2σ (equivalent to 95.4% confidence interval) calculated as a percentage
    let two_sigma_percent = if self.mean > 0.0 { (2.0 * self.std_dev / self.mean) * 100.0 } else { 0.0 };
    f.write_fmt(format_args!(
      "{}: {} ±{:.1}% [{}|{}|{}]",
      self.count,
      self.unit.format(self.mean),
      two_sigma_percent,
      self.unit.short(self.min),
      self.unit.short(self.median),
      self.unit.short(self.max)
    ))?;
    Ok(())
  }
}

#[derive(Debug, Clone, Copy)]
pub enum Unit {
  Bytes,
  Milliseconds,
}

impl Unit {
  fn scaled_format(mut value: f64, scale: usize, unit: &str, auxs: &[&str], precision: usize) -> String {
    let mut unit_index = 0;
    while value >= scale as f64 && unit_index + 1 < auxs.len() {
      value /= scale as f64;
      unit_index += 1;
    }
    format!("{:.precision$}{}{}", value, auxs[unit_index], unit, precision = precision)
  }
  fn format(&self, value: f64) -> String {
    match self {
      Self::Bytes => Self::scaled_format(value, 1024, "B", &["", "k", "M", "G", "T", "P"], 2),
      Self::Milliseconds => Self::scaled_format(value * 1000.0 * 1000.0, 1000, "s", &["n", "μ", "m", ""], 2),
    }
  }
  fn short(&self, value: f64) -> String {
    match self {
      Self::Bytes => Self::scaled_format(value, 1024, "", &["", "k", "M", "G", "T", "P"], 0),
      Self::Milliseconds => Self::scaled_format(value * 1000.0 * 1000.0, 1000, "", &["n", "μ", "m", ""], 0),
    }
  }
}

pub struct XYReport<X: Display + Clone + std::hash::Hash + Eq + PartialEq + Ord, Y: IntoFloat + Display> {
  unit: Unit,
  data_set: HashMap<X, Vec<Y>>,
}

impl<X: Display + Clone + std::hash::Hash + Eq + PartialEq + Ord, Y: IntoFloat + Display> XYReport<X, Y> {
  pub fn new(unit: Unit) -> Self {
    XYReport { unit, data_set: HashMap::new() }
  }

  pub fn add(&mut self, x: &X, y: Y) -> Stat {
    self.append(x, vec![y])
  }

  pub fn append(&mut self, x: &X, mut ys: Vec<Y>) -> Stat {
    self.data_set.entry(x.clone()).or_default().append(&mut ys);
    self.calculate(x).unwrap()
  }

  pub fn save_xy_to_csv(&self, path: &PathBuf, x_label: &str, y_labels: &str) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "{x_label},{y_labels}")?;

    let mut xs = self.data_set.keys().cloned().collect::<Vec<_>>();
    xs.sort_unstable();
    for x in xs.iter() {
      let ys = self.data_set.get(x).unwrap().iter().map(|f| format!("{f}")).collect::<Vec<_>>();
      writeln!(writer, "{},{}", x, ys.join(","))?;
    }

    writer.flush()?;
    Ok(())
  }

  pub fn max_cv(&self) -> f64 {
    if self.data_set.is_empty() {
      return f64::NAN;
    }
    let mut max = 0.0;
    for x in self.data_set.keys() {
      let r = self.calculate(x).unwrap().cv();
      if r.is_nan() || r.is_infinite() {
        return r;
      }
      if r > max {
        max = r;
      }
    }
    max
  }

  pub fn is_cv_sufficient(&self, x: X, cv: f64) -> bool {
    match self.data_set.get(&x).map(|ys| Stat::from_vec(self.unit, ys)) {
      Some(stat) => {
        if stat.count <= 2 {
          false
        } else {
          stat.cv() < cv
        }
      }
      None => false,
    }
  }

  pub fn calculate(&self, x: &X) -> Option<Stat> {
    self.data_set.get(x).map(|ys| Stat::from_vec(self.unit, ys))
  }
}

pub struct ExpirationTimer {
  start: Instant,
  dead_line: Duration,
  last_noticed: Instant,
  notice_interval: Duration,
  max_trials: usize,
  current: usize,
  interval: usize,
}

impl ExpirationTimer {
  pub fn new(dead_line: Duration, minutes: usize, max_trials: usize, div: usize) -> Self {
    let start = Instant::now();
    let last_noticed = start;
    let notice_interval = Duration::from_secs(minutes as u64 * 60);
    let current = 0;
    let interval = max_trials / div;
    Self { start, dead_line, last_noticed, notice_interval, max_trials, current, interval }
  }

  pub fn expired(&self) -> bool {
    self.start.elapsed() >= self.dead_line
  }

  pub fn elapsed(&self) -> Duration {
    self.start.elapsed()
  }

  pub fn estimated_end_time(&self) -> Instant {
    if self.current == 0 {
      Instant::now() + Duration::from_secs(365 * 24 * 60 * 60)
    } else {
      let avr_per_trial = self.elapsed() / self.current as u32;
      let total_estimate = avr_per_trial * self.max_trials as u32;
      self.start + total_estimate
    }
  }

  pub fn eta(&self) -> String {
    let system_time = SystemTime::now() + (self.estimated_end_time() - Instant::now());
    let dt: DateTime<Local> = system_time.into();
    let now: DateTime<Local> = SystemTime::now().into();
    let diff = dt - now;
    let fmt = if now.date_naive() != dt.date_naive() {
      "%m-%d %H:%M"
    } else if diff.num_hours() >= 1 {
      "%H:%M"
    } else {
      "%H:%M:%S"
    };
    let eta = dt.format(fmt).to_string();

    let secs = diff.num_seconds();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    let remaining = if h > 0 {
      format!("{h}h{m:02}m")
    } else if m > 0 {
      format!("{m}m{s:02}s")
    } else {
      format!("{s}s")
    };
    format!("{eta} ({remaining})")
  }

  pub fn carried_out(&mut self, amount: usize) -> bool {
    let current = self.current;
    self.current += amount;

    if (self.last_noticed.elapsed() >= self.notice_interval)
      || self.current >= self.max_trials
      || (current != 0 && (self.current / self.interval != current / self.interval))
    {
      self.last_noticed = Instant::now();
      true
    } else {
      false
    }
  }

  fn heading(columns: &[Column]) {
    println!("{}", columns.iter().map(|c| c.heading()).collect::<Vec<_>>().join(" "));
    println!("{}", columns.iter().map(|c| c.line()).collect::<Vec<_>>().join(" "));
  }

  fn summary(columns: &[Column]) {
    println!("{}", columns.iter().map(|c| c.fmt()).collect::<Vec<_>>().join(" "));
  }

  pub fn heading_ms() {
    Self::heading(&[
      Column::DataSize(0),
      Column::MeanMS(0.0),
      Column::StdDevMS(0.0),
      Column::CV(0.0),
      Column::Trials(0),
      Column::Eta(String::from("")),
    ]);
  }
  pub fn summary_ms(&self, data_size: u64, mean: f64, std_dev: f64) {
    Self::summary(&[
      Column::DataSize(data_size),
      Column::MeanMS(mean),
      Column::StdDevMS(std_dev),
      Column::CV(std_dev / mean * 100.0),
      Column::Trials(self.current),
      Column::Eta(self.eta()),
    ]);
  }
  pub fn heading_max_cv() {
    Self::heading(&[Column::DataSize(0), Column::CV(0.0), Column::Trials(0), Column::Eta(String::from(""))]);
  }
  pub fn summary_max_cv(&self, data_size: u64, max_cv: f64) {
    Self::summary(&[
      Column::DataSize(data_size),
      Column::CV(max_cv * 100.0),
      Column::Trials(self.current),
      Column::Eta(self.eta()),
    ]);
  }
}

enum Column {
  DataSize(u64),
  MeanMS(f64),
  StdDevMS(f64),
  CV(f64),
  Trials(usize),
  Eta(String),
}

impl Column {
  pub fn label(&self) -> &'static str {
    match self {
      Self::DataSize(_) => "DataSize",
      Self::MeanMS(_) => "Mean[ms]",
      Self::StdDevMS(_) => "StdDev[ms]",
      Self::CV(_) => "CV[%]",
      Self::Trials(_) => "Trials",
      Self::Eta(_) => "ETA",
    }
  }
  pub fn len(&self) -> usize {
    self.label().len().max(match self {
      Self::DataSize(_) => 10,
      Self::MeanMS(_) => 12,
      Self::StdDevMS(_) => 12,
      Self::CV(_) => 6,
      Self::Trials(_) => 9,
      Self::Eta(_) => 18,
    })
  }

  pub fn heading(&self) -> String {
    let h = match self {
      Self::DataSize(_) => "DataSize",
      Self::MeanMS(_) => "Mean[ms]",
      Self::StdDevMS(_) => "StdDev[ms]",
      Self::CV(_) => "CV[%]",
      Self::Trials(_) => "Trials",
      Self::Eta(_) => "ETA",
    };
    format!("{h:^s$}", s = self.len())
  }

  pub fn line(&self) -> String {
    "-".repeat(self.len())
  }

  pub fn fmt(&self) -> String {
    match self {
      Self::DataSize(ds) => format!("{ds:>w$}", w = self.len()),
      Self::MeanMS(m) => format!("{m:>w$.3}", w = self.len()),
      Self::StdDevMS(sd) => format!("{sd:>w$.3}", w = self.len()),
      Self::CV(cv) => format!("{cv:>w$.1}", w = self.len()),
      Self::Trials(tr) => format!("{tr:>w$}", w = self.len()),
      Self::Eta(eta) => format!("{eta:<w$}", w = self.len()),
    }
  }
}

use crate::IntoFloat;
use core::f64;
use slate::Result;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

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

pub struct Report<X: Display + Copy + std::hash::Hash + Eq + PartialEq + Ord, Y: IntoFloat + Display> {
  unit: Unit,
  data_set: HashMap<X, Vec<Y>>,
}

impl<X: Display + Copy + std::hash::Hash + Eq + PartialEq + Ord, Y: IntoFloat + Display> Report<X, Y> {
  pub fn new(unit: Unit) -> Self {
    Report { unit, data_set: HashMap::new() }
  }

  pub fn add(&mut self, x: X, y: Y) -> Stat {
    self.append(x, vec![y])
  }

  pub fn append(&mut self, x: X, mut ys: Vec<Y>) -> Stat {
    self.data_set.entry(x).or_default().append(&mut ys);
    self.calculate(x).unwrap()
  }

  pub fn save_xy_to_csv(&self, path: &PathBuf, x_label: &str, y_labels: &str) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "{x_label},{y_labels}")?;

    let mut xs = self.data_set.keys().copied().collect::<Vec<_>>();
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
      let r = self.calculate(*x).unwrap().cv();
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

  pub fn calculate(&self, x: X) -> Option<Stat> {
    self.data_set.get(&x).map(|ys| Stat::from_vec(self.unit, ys))
  }
}

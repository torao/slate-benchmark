use core::f64;
use slate::Result;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Stat {
  pub count: usize,
  pub mean: f64,
  pub median: f64,
  pub std_dev: f64,
  pub min: f64,
  pub max: f64,
}

impl Stat {
  pub fn from_vec(mut data: Vec<f64>) -> Stat {
    if data.is_empty() {
      return Stat { count: 0, mean: f64::NAN, median: f64::NAN, std_dev: f64::NAN, min: f64::NAN, max: f64::NAN };
    }
    let count = data.len();
    let min = *data.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let max = *data.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let sum = data.iter().sum::<f64>();
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
    Stat { count, mean, median, std_dev, min, max }
  }
}

impl Display for Stat {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    // 2σ (equivalent to 95.4% confidence interval) calculated as a percentage
    let two_sigma_percent = if self.mean > 0.0 { (2.0 * self.std_dev / self.mean) * 100.0 } else { 0.0 };
    f.write_fmt(format_args!(
      "{}: {:.2}ms ±{:.1}% [{:.1}|{:.1}|{:.1}]",
      self.count, self.mean, two_sigma_percent, self.min, self.median, self.max
    ))?;
    Ok(())
  }
}

pub struct Report<X: Display + Copy + std::hash::Hash + Eq + PartialEq + Ord> {
  data_set: HashMap<X, Vec<f64>>,
}

impl<X: Display + Copy + std::hash::Hash + Eq + PartialEq + Ord> Report<X> {
  pub fn new() -> Self {
    Report { data_set: HashMap::new() }
  }
  pub fn add(&mut self, x: X, y: Duration) {
    self.data_set.entry(x).or_default().push(y.as_micros() as f64 / 1000.0);
  }
  pub fn single(&self, x: X) -> Stat {
    self.calc().get(&x).unwrap().clone()
  }
  pub fn save_to_csv(&self, path: &PathBuf) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "N,COUNT,MEAN,MEDIAN,STDDEV,MIN,MAX")?;

    let ss = self.calc();
    let mut xs = ss.keys().copied().collect::<Vec<_>>();
    xs.sort_unstable();
    for x in xs.iter() {
      let y = ss.get(x).unwrap();
      writeln!(
        writer,
        "\"{}\",{},{:.3},{:.3},{:.3},{:.3},{:.3}",
        x, y.count, y.mean, y.median, y.std_dev, y.min, y.max
      )?;
    }

    writer.flush()?;
    Ok(())
  }
  fn calc(&self) -> HashMap<X, Stat> {
    self
      .data_set
      .iter()
      .map(|(x, ys)| {
        let s = Stat::from_vec(ys.clone());
        (*x, s)
      })
      .collect()
  }
}

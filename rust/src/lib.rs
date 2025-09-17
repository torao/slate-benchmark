use core::f64;
use std::collections::HashMap;
use std::fs::{OpenOptions, metadata, read_dir};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use slate::{Position, Result, Serializable, Storage};

pub mod hashtree;

#[derive(Debug)]
pub struct MemKVS<S: Serializable + Clone + 'static> {
  kvs: Arc<RwLock<HashMap<Position, S>>>,
}

struct MemKVSReader<S: Serializable + 'static> {
  kvs: Arc<RwLock<HashMap<Position, S>>>,
}

impl<S: Serializable + Clone + 'static> MemKVS<S> {
  pub fn new() -> Self {
    Self::with_kvs(Default::default())
  }

  pub fn with_kvs(kvs: Arc<RwLock<HashMap<Position, S>>>) -> Self {
    Self { kvs }
  }
}

impl<S: Serializable + Clone + 'static> Default for MemKVS<S> {
  fn default() -> Self {
    Self::new()
  }
}

impl<S: Serializable + Clone + 'static> Storage<S> for MemKVS<S> {
  fn first(&mut self) -> Result<(Option<S>, slate::Position)> {
    let kvs = self.kvs.read()?;
    let n = kvs.len() as Position;
    Ok((kvs.get(&n).cloned(), n + 1))
  }

  fn last(&mut self) -> Result<(Option<S>, slate::Position)> {
    let kvs = self.kvs.read()?;
    let n = kvs.len() as Position;
    if n == 0 { Ok((None, 1)) } else { Ok((kvs.get(&n).cloned(), n + 1)) }
  }

  fn put(&mut self, position: Position, data: &S) -> Result<slate::Position> {
    let mut kvs = self.kvs.write()?;
    kvs.insert(position, data.clone());
    Ok(kvs.len() as Position + 1)
  }

  fn reader(&self) -> Result<Box<dyn slate::Reader<S>>> {
    Ok(Box::new(MemKVSReader { kvs: self.kvs.clone() }))
  }
}

impl<S: Serializable + Clone> slate::Reader<S> for MemKVSReader<S> {
  fn read(&mut self, position: Position) -> Result<S> {
    let kvs = self.kvs.read()?;
    Ok(kvs.get(&position).cloned().unwrap())
  }
}

pub struct ZipfSampler {
  state: u64,
  n: u64,
  head_cdf: Vec<f64>,
  tails: f64,
}

impl ZipfSampler {
  /// パラメータ s の効果：
  /// 0.5: 軽微な偏り
  /// 1.0: 中程度の偏り
  /// 1.5: 強い偏り (推奨)
  /// 2.0: 非常に強い偏り
  pub fn new(seed: u64, s: f64, n: u64) -> Self {
    assert!(s > 0.0);
    assert!(n >= 1);

    // n=2G のような巨大なデータセットに対して事前計算するため、前方のみの CDF を算出し、ほとんど変化のない
    // テールは固定値として保持する。s=0.5～2.0 では数千個程度の値が保持される
    let min_samples = 1000;
    let convergence_threshold = 1.0 / 1000.0;
    let mut head_cdf = Vec::with_capacity(min_samples);
    let mut cumulative = 0.0;
    let mut prev_p = f64::INFINITY;
    for i in 1..=n {
      let p = 1.0 / (i as f64).powf(s);
      cumulative += p;
      head_cdf.push(cumulative);
      if i > min_samples as u64 && (prev_p - p) / prev_p < convergence_threshold {
        break;
      }
      prev_p = p;
    }

    // 正規化
    let cutoff_index = head_cdf.len() as u64;
    let tail_mass =
      if cutoff_index < n { (cutoff_index + 1..=n).map(|i| 1.0 / (i as f64).powf(s)).sum::<f64>() } else { 0.0 };
    let total_mass = cumulative + tail_mass;
    for p in &mut head_cdf {
      *p /= total_mass;
    }
    let tails = cumulative / total_mass;

    Self { state: seed, n, head_cdf, tails }
  }

  pub fn next_u64(&mut self) -> u64 {
    // (0, 1] 範囲の一様乱数を生成
    self.state = splitmix64(self.state);
    let u = ((self.state >> 11) as f64) / ((1u64 << 53) as f64);

    // (1, n) 範囲の Zipf 分布に従う乱数を生成
    let i = if u <= self.tails {
      // 二分探索で対応するインデックスを取得
      match self.head_cdf.binary_search_by(|p| p.partial_cmp(&u).unwrap()) {
        Ok(i) | Err(i) => (i + 1) as u64,
      }
    } else {
      let tail_u = (u - self.tails) / (1.0 - self.tails);
      let tail_range = self.n - self.head_cdf.len() as u64;
      self.head_cdf.len() as u64 + 1 + (tail_u * tail_range as f64) as u64
    };
    self.n - i + 1
  }
}

pub fn unique_file(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
  for i in 0..=usize::MAX {
    let name = if i == 0 { format!("{prefix}{suffix}") } else { format!("{prefix}_{i}{suffix}") };
    let path = dir.join(name);
    if !path.exists() && OpenOptions::new().write(true).create_new(true).open(&path).is_ok() {
      assert!(path.is_file());
      return path;
    }
  }
  panic!("Temporary file name space is full: {prefix}_nnn{suffix}");
}

pub fn file_size<P: AsRef<Path>>(path: P) -> u64 {
  if path.as_ref().is_file() {
    metadata(&path).map(|m| m.len()).unwrap_or(0)
  } else if path.as_ref().is_dir() {
    read_dir(path)
      .unwrap()
      .flat_map(std::result::Result::ok)
      .map(|e| {
        let path = e.path();
        if path.is_dir() { file_size(&path) } else { metadata(&path).map(|m| m.len()).unwrap_or(0) }
      })
      .sum()
  } else {
    0
  }
}

pub fn splitmix64(x: u64) -> u64 {
  let mut z = x;
  z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
  z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
  z ^ (z >> 31)
}

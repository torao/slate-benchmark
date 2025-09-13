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

pub struct ZipfDistribution {
  state: u64,
  n: u64,
  cdf: Vec<f64>,
}

/// パラメータ s の効果：
/// 0.5: 軽微な偏り
/// 1.0: 中程度の偏り
/// 1.5: 強い偏り (推奨)
/// 2.0: 非常に強い偏り
impl ZipfDistribution {
  pub fn new(seed: u64, s: f64, n: u64) -> Self {
    assert!(s > 0.0);
    assert!(n >= 1);

    // 各値の累積確率分布を算出
    let mut cdf = Vec::with_capacity(n as usize);
    let mut sum = 0.0;
    for k in 1..=n {
      let p = (k as f64).powf(-s);
      sum += p;
      cdf.push(sum);
    }
    for p in &mut cdf {
      *p /= sum; // 正規化
    }

    Self { state: seed, n, cdf }
  }

  pub fn next_u64(&mut self) -> u64 {
    // (0, 1] 範囲の一様乱数を生成
    self.state = splitmix64(self.state);
    let u = ((self.state >> 11) as f64) / ((1u64 << 53) as f64);

    // 二分探索で対応するインデックスを取得
    match self.cdf.binary_search_by(|p| p.partial_cmp(&u).unwrap()) {
      Ok(i) => self.n - i as u64,
      Err(i) => self.n - i as u64 + 1,
    }
  }
}

pub struct ParetoDistribution {
  seed: u64,
  alpha: f64, // shape parameter > 0
}

impl ParetoDistribution {
  pub fn new(seed: u64, alpha: f64) -> Self {
    assert!(alpha > 0.0);
    Self { seed, alpha }
  }

  /// 1..=n 範囲で切り詰められた (上限 n) パレート分布に従う整数を生成します。
  pub fn next_u64(&mut self, n: u64) -> u64 {
    assert!(n >= 1);

    // (0, 1] 範囲の一様乱数を生成
    self.seed = splitmix64(self.seed);
    let r = self.seed; // in [0, u64::MAX]
    let u = (r as f64 + 1.0) / (u128::from(u64::MAX) as f64 + 1.0); // in (0,1]

    let denom = 1.0 - (n as f64).powf(-self.alpha);
    let x_continuous = if n == 1 {
      1.0
    } else {
      let inner = 1.0 - u * denom;
      let inner = inner.max((n as f64).powf(-self.alpha)).min(1.0); // inner should be in (n^{-alpha}, 1.0]; guard numeric issues
      inner.powf(-1.0 / self.alpha)
    };

    let mut k = x_continuous.floor() as i128;
    if k < 1 {
      k = 1;
    }
    if k as u64 > n {
      k = n as i128;
    }
    k as u64
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

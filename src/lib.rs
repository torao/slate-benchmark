use std::fs::{metadata, read_dir};
use std::path::Path;

#[inline]
pub fn u64_to_rand_bytes(value: u64, buffer: &mut [u8; 8]) {
  // SplitMix64
  let mut z = value + 0x9e3779b97f4a7c15;
  z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
  z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
  let z = z ^ (z >> 31);

  buffer.copy_from_slice(&z.to_le_bytes());
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

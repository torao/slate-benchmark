use slate::Result;
use slate_benchmark::u64_to_rand_bytes;
use std::fs::File;
use std::io::Write;

#[inline(never)]
fn single_append(file: &mut File, buffer: &[u8]) -> Result<()> {
  file.write_all(buffer)?;
  Ok(file.flush()?)
}

#[inline(never)]
pub fn append(file: &mut File, n: u32) -> Result<()> {
  let mut buffer = [0u8; 8];
  for i in 0u32..n {
    u64_to_rand_bytes(i as u64, &mut buffer);
    single_append(file, &buffer).unwrap();
  }
  Ok(())
}

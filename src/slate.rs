use slate::{FileStorage, Result, Slate};
use slate_benchmark::u64_to_rand_bytes;

#[inline(never)]
pub fn append(slate: &mut Slate<FileStorage>, n: u32) -> Result<()> {
  let mut buffer = [0u8; 8];
  for i in 0u32..n {
    u64_to_rand_bytes(i as u64, &mut buffer);
    slate.append(&buffer).unwrap();
  }
  Ok(())
}

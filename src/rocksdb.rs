use rocksdb::DB;
use slate::Result;
use slate_benchmark::u64_to_rand_bytes;

pub fn append(db: &DB, n: u32) -> Result<()> {
  let mut key = [0u8; 4];
  let mut value = [0u8; 8];
  for i in 0u32..n {
    key.copy_from_slice(&i.to_le_bytes());
    u64_to_rand_bytes(i as u64, &mut value);
    db.put(key, value).unwrap();
  }
  Ok(())
}

use blake3::Hash;

pub mod binary;

/// Core hash tree abstraction
pub trait HashTree {
  type Error;

  /// Append a new data item to the tree
  fn append(&mut self, data: Vec<u8>) -> Result<u64, Self::Error>;

  /// Retrieve data by index
  fn get(&mut self, index: u64) -> Result<Option<Vec<u8>>, Self::Error>;

  /// Get the current size (number of leaf nodes)
  fn size(&self) -> u64;

  /// Get the root hash
  fn root_hash(&mut self) -> Result<Hash, Self::Error>;

  /// Verify a path from leaf to root
  fn verify_path(&mut self, index: u64, data: &[u8], proof: &[Hash]) -> Result<bool, Self::Error>;

  /// Generate proof path for given index
  fn generate_proof(&mut self, index: u64) -> Result<Vec<Hash>, Self::Error>;

  /// Sync changes to persistent storage
  fn sync(&mut self) -> Result<(), Self::Error>;
}

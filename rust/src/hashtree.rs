use slate::{Entry, Slate, Storage};

pub mod binary;

/// Core hash tree abstraction
pub trait HashTree {
  type Error;

  /// Get the current size (number of leaf nodes)
  fn size(&self) -> u64;

  /// Retrieve data by index
  fn get(&mut self, index: u64) -> Result<Option<Vec<u8>>, Self::Error>;
}

pub struct SlateHashTree<S: Storage<Entry>>(Slate<S>);

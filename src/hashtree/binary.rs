use blake3::{Hash, OUT_LEN};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use slate::Serializable;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::hashtree::HashTree;

pub const MAX_DATA_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub enum NodeKind {
  Leaf { data: Vec<u8> },
  Branch { left: u64, right: u64 },
}

/// Node representation in the hash tree
#[derive(Debug, Clone)]
pub struct Node {
  pub position: u64,
  pub hash: Hash,
  pub range: (u64, u64),
  pub kind: NodeKind,
}

impl Node {
  pub fn new_leaf(id: u64, position: u64, data: Vec<u8>) -> Self {
    assert!(data.len() <= MAX_DATA_SIZE);
    let hash = blake3::hash(&data);
    let range = (id, id);
    let leaf = NodeKind::Leaf { data };
    Node { position, hash, range, kind: leaf }
  }

  pub fn new_internal(position: u64, left: &Node, right: &Node) -> Self {
    assert!(left.range.1 + 1 == right.range.0);
    let range = (left.range.0, right.range.1);
    let hash = Self::combine(&left.hash, &right.hash);
    let branch = NodeKind::Branch { left: left.position, right: right.position };
    Node { position, hash, range, kind: branch }
  }

  pub fn is_leaf(&self) -> bool {
    self.range.0 == self.range.1
  }

  pub fn level(&self) -> u8 {
    let n = self.range.1 - self.range.0 + 1;
    (u64::BITS - (n - 1).leading_zeros()) as u8
  }

  pub fn contains(&self, id: u64) -> bool {
    self.range.0 <= id && id <= self.range.1
  }

  fn combine(left: &Hash, right: &Hash) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(left.as_bytes());
    hasher.update(right.as_bytes());
    hasher.finalize()
  }
}

impl Serializable for Node {
  fn write<W: Write>(&self, w: &mut W) -> slate::Result<usize> {
    // Hash (32 bytes)
    w.write_all(self.hash.as_bytes()).unwrap();

    // Range (16 byte)
    w.write_u64::<LittleEndian>(self.range.0).unwrap();
    w.write_u64::<LittleEndian>(self.range.1).unwrap();

    let len = match &self.kind {
      NodeKind::Leaf { data } => {
        // Data length and data (if leaf)
        w.write_u32::<LittleEndian>(data.len() as u32).unwrap();
        let mut buffer = [0u8; MAX_DATA_SIZE];
        Cursor::new(&mut buffer[..]).write_all(data).unwrap();
        w.write_all(&buffer).unwrap();
        4 + MAX_DATA_SIZE
      }
      NodeKind::Branch { left, right } => {
        // Children indices (8 bytes each)
        w.write_u64::<LittleEndian>(*left).unwrap();
        w.write_u64::<LittleEndian>(*right).unwrap();
        8 + 8
      }
    };
    Ok(OUT_LEN + 16 + len)
  }

  fn read<R: Read + Seek>(r: &mut R, position: slate::Position) -> slate::Result<Self> {
    // Hash
    let mut hash_bytes = [0u8; OUT_LEN];
    r.read_exact(&mut hash_bytes)?;
    let hash = Hash::from(hash_bytes);

    // Range
    let min = r.read_u64::<LittleEndian>()?;
    let max = r.read_u64::<LittleEndian>()?;

    let kind = if min == max {
      // Data
      let data_len = r.read_u32::<LittleEndian>()? as usize;
      let mut buffer = [0u8; MAX_DATA_SIZE];
      r.read_exact(&mut buffer[..])?;
      let data = buffer[..data_len].to_vec();
      NodeKind::Leaf { data }
    } else {
      // Children
      let left = r.read_u64::<LittleEndian>()?;
      let right = r.read_u64::<LittleEndian>()?;
      NodeKind::Branch { left, right }
    };

    Ok(Node { position, range: (min, max), hash, kind })
  }
}

type Result<T> = std::result::Result<T, BinaryHashTreeError>;

#[derive(Debug)]
pub enum BinaryHashTreeError {
  IoError(std::io::Error),
  SlateError(slate::error::Error),
  InvalidData(String),
  NodeNotFound(u64),
}

impl From<std::io::Error> for BinaryHashTreeError {
  fn from(err: std::io::Error) -> Self {
    BinaryHashTreeError::IoError(err)
  }
}

impl From<slate::error::Error> for BinaryHashTreeError {
  fn from(err: slate::error::Error) -> Self {
    BinaryHashTreeError::SlateError(err)
  }
}

/// Binary Hash Tree implementation with file-based storage
pub struct BinaryHashTree {
  file: File,
  root_position: Option<u64>,
  size: u64,
  cache: Cache, // In-memory cache
}

const METADATA_SIZE: usize = 8 + 8; // size(8) + root_position(8)

impl BinaryHashTree {
  /// Create a new binary hash tree with file storage
  pub fn new<P: AsRef<Path>>(path: P, cache_limit: usize) -> Result<Self> {
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    let root_position = None;
    let size = 0;
    let cache = Cache::new(cache_limit);
    let mut tree = BinaryHashTree { file, root_position, size, cache };
    tree.load_metadata()?;
    Ok(tree)
  }

  /// Load metadata from file header
  fn load_metadata(&mut self) -> Result<()> {
    let mut metadata = [0u8; METADATA_SIZE];
    self.file.seek(SeekFrom::Start(0))?;
    match self.file.read_exact(&mut metadata) {
      Ok(_) => {
        let mut cursor = Cursor::new(&metadata);
        let size = cursor.read_u64::<LittleEndian>()?;
        let root_position = cursor.read_u64::<LittleEndian>()?;
        self.size = size;
        self.root_position = if self.size > 0 { Some(root_position) } else { None };
      }
      Err(_) => {
        // New file, write initial metadata
        self.save_metadata()?;
      }
    }
    self.file.seek(SeekFrom::End(0))?;
    Ok(())
  }

  /// Save metadata to file header
  fn save_metadata(&mut self) -> Result<()> {
    let mut metadata = vec![0u8; METADATA_SIZE];
    let mut cursor = Cursor::new(&mut metadata);
    cursor.write_u64::<LittleEndian>(self.size)?;
    cursor.write_u64::<LittleEndian>(self.root_position.unwrap_or(0))?;
    self.file.write_all(&metadata)?;
    Ok(())
  }

  /// Write node to file and return its position
  fn write_node(&mut self, node: &Node) -> Result<()> {
    self.file.seek(SeekFrom::Start(node.position))?;
    let mut bw = BufWriter::new(&mut self.file);
    node.write(&mut bw)?;
    bw.flush()?;
    println!("WRITE: {node:?}");

    // Cache the node if it's a higher level
    if self.cache.replace(node) {
      println!("CACHED: {:?}", node.position);
    }

    Ok(())
  }

  /// Read node from file
  fn read_node(&mut self, position: u64) -> Result<Node> {
    assert!(position != 0);

    // Check cache first
    if let Some(node) = self.cache.get(position) {
      println!("CACHE HIT: {:?}", node.position);
      return Ok(node.clone());
    }

    self.file.seek(SeekFrom::Start(position))?;
    let mut br = BufReader::new(&mut self.file);
    let node = Node::read(&mut br, position)?;
    println!("READ: {node:?}");
    if self.cache.offer(&node) {
      println!("CACHED: {:?}", node.position);
    }
    Ok(node)
  }

  fn root(&mut self) -> Result<Option<Node>> {
    if let Some(position) = self.root_position { Ok(Some(self.read_node(position)?)) } else { Ok(None) }
  }

  fn lookup(&mut self, id: u64, node: Node) -> Result<Node> {
    assert!(node.contains(id));
    match &node.kind {
      NodeKind::Leaf { .. } => Ok(node),
      NodeKind::Branch { left, right } => {
        let left = self.read_node(*left)?;
        if left.contains(id) {
          self.lookup(id, left)
        } else {
          let right = self.read_node(*right)?;
          self.lookup(id, right)
        }
      }
    }
  }

  fn update(&mut self, id: u64, mut node: Node, value: &[u8]) -> Result<Hash> {
    assert!(node.contains(id));
    match &node.kind {
      NodeKind::Leaf { .. } => {
        let leaf = Node::new_leaf(id, node.position, value.to_vec());
        self.write_node(&leaf)?;
        Ok(leaf.hash)
      }
      NodeKind::Branch { left, right } => {
        let left = self.read_node(*left)?;
        let right = self.read_node(*right)?;
        let (lhash, rhash) = if left.contains(id) {
          let lhash = self.update(id, left, value)?;
          (lhash, right.hash)
        } else {
          let rhash = self.update(id, right, value)?;
          (left.hash, rhash)
        };
        node.hash = Node::combine(&lhash, &rhash);
        self.write_node(&node)?;
        Ok(node.hash)
      }
    }
  }

  /// Build tree by expanding with zero padding
  fn expand(&mut self, root: Node, leaf: Node) -> Result<()> {
    let mut nodes = Vec::with_capacity(self.size as usize);

    // Store all of the leaf nodes of next subtree
    self.write_node(&leaf)?;
    nodes.push(leaf);
    for i in 1..self.size {
      let id = self.size + i;
      let position = self.file.stream_position()?;
      let empty_leaf = Node::new_leaf(id, position, [].to_vec());
      self.write_node(&empty_leaf)?;
      nodes.push(empty_leaf);
    }

    // Store all of the branches of next subtree
    while nodes.len() > 1 {
      for i in (0..nodes.len()).step_by(2) {
        let left = &nodes[i];
        let right = &nodes[i + 1];
        let position = self.file.stream_position()?;
        let branch = Node::new_internal(position, left, right);
        self.write_node(&branch)?;
        nodes[i / 2] = branch;
      }
      nodes.truncate(nodes.len() / 2);
    }

    // Store the new root node
    let left = root;
    let right = nodes.first().unwrap().clone();
    let position = self.file.stream_position()?;
    let root = Node::new_internal(position, &left, &right);
    self.write_node(&root)?;
    self.root_position = Some(position);
    Ok(())
  }
}

impl HashTree for BinaryHashTree {
  type Error = BinaryHashTreeError;

  fn append(&mut self, data: Vec<u8>) -> Result<u64> {
    let id = self.size();
    match self.root()? {
      None => {
        assert!(self.size == 0);
        let position = self.file.seek(SeekFrom::End(0))?;
        let leaf = Node::new_leaf(id, position, data);
        self.write_node(&leaf)?;

        self.root_position = Some(position);
      }
      Some(root) => {
        if root.contains(id) {
          self.update(id, root.clone(), &data)?;
        } else {
          let position = self.file.seek(SeekFrom::End(0))?;
          let leaf = Node::new_leaf(id, position, data);
          self.expand(root, leaf)?;
        }
      }
    }
    self.size += 1;
    self.save_metadata()?;

    Ok(id)
  }

  fn get(&mut self, index: u64) -> Result<Option<Vec<u8>>> {
    if index >= self.size {
      Ok(None)
    } else if let Some(root) = self.root()? {
      let leaf = self.lookup(index, root)?;
      match &leaf.kind {
        NodeKind::Leaf { data } => Ok(Some(data.clone())),
        _ => unreachable!(),
      }
    } else {
      Ok(None)
    }
  }

  fn size(&self) -> u64 {
    self.size
  }

  fn root_hash(&mut self) -> Result<Hash> {
    if let Some(root) = self.root()? { Ok(root.hash) } else { Ok(blake3::hash(&[])) }
  }

  fn verify_path(&mut self, _index: u64, _data: &[u8], _proof: &[Hash]) -> Result<bool> {
    unimplemented!()
  }

  fn generate_proof(&mut self, _index: u64) -> Result<Vec<Hash>> {
    unimplemented!()
  }

  fn sync(&mut self) -> Result<()> {
    self.file.sync_all()?;
    Ok(())
  }
}

/// A cache that prioritizes the storing of higher-level nodes.
struct Cache {
  cache: HashMap<u64, Node>,
  limit: usize,
  min_level: u8,
}

impl Cache {
  fn new(limit: usize) -> Self {
    assert!(limit > 0);
    Self { cache: HashMap::default(), limit, min_level: 0 }
  }

  fn offer(&mut self, node: &Node) -> bool {
    if (node.level() <= self.min_level && self.cache.len() == self.limit) || self.cache.contains_key(&node.position) {
      return false;
    }
    self.cache.insert(node.position, node.clone());
    if self.cache.len() > self.limit {
      let mut entries = self.cache.iter().collect::<Vec<_>>();
      entries.sort_by_key(|(_, node)| node.level());
      let min = entries[self.cache.len() - self.limit].1.level();
      let keys = entries.iter().take(self.cache.len() - self.limit).map(|(key, _)| **key).collect::<Vec<_>>();
      for key in keys {
        self.cache.remove(&key);
      }
      self.min_level = min;
    } else {
      self.min_level = std::cmp::min(self.min_level, node.level());
    }
    return true;
  }

  fn replace(&mut self, node: &Node) -> bool {
    if let std::collections::hash_map::Entry::Occupied(mut e) = self.cache.entry(node.position) {
      e.insert(node.clone());
      return true;
    }
    self.offer(node)
  }

  fn get(&self, position: u64) -> Option<&Node> {
    self.cache.get(&position)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::NamedTempFile;

  #[test]
  fn test_basic_operations() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut tree = BinaryHashTree::new(temp_file.path(), 3).unwrap();

    // Test append
    let index1 = tree.append(b"hello".to_vec()).unwrap();
    let index2 = tree.append(b"world".to_vec()).unwrap();
    let index3 = tree.append(b"test".to_vec()).unwrap();

    assert_eq!(index1, 0);
    assert_eq!(index2, 1);
    assert_eq!(index3, 2);
    assert_eq!(tree.size(), 3);

    // Test retrieval
    assert_eq!(tree.get(0).unwrap(), Some(b"hello".to_vec()));
    assert_eq!(tree.get(1).unwrap(), Some(b"world".to_vec()));
    assert_eq!(tree.get(2).unwrap(), Some(b"test".to_vec()));
    assert_eq!(tree.get(3).unwrap(), None);

    // Test proof generation and verification
    // let proof = tree.generate_proof(1).unwrap();
    // assert!(tree.verify_path(1, b"world", &proof).unwrap());
    // assert!(!tree.verify_path(1, b"wrong", &proof).unwrap());
  }

  #[test]
  fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    // Create tree and add data
    {
      let mut tree = BinaryHashTree::new(&path, 3).unwrap();
      tree.append(b"persistent".to_vec()).unwrap();
      tree.append(b"data".to_vec()).unwrap();
      tree.sync().unwrap();
    }

    // Reload tree and verify data
    {
      let mut tree = BinaryHashTree::new(&path, 3).unwrap();
      assert_eq!(tree.size(), 2);
      assert_eq!(tree.get(0).unwrap(), Some(b"persistent".to_vec()));
      assert_eq!(tree.get(1).unwrap(), Some(b"data".to_vec()));
    }
  }
}

use blake3::{Hash, Hasher, OUT_LEN};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use slate::file::FileDevice;
use slate::formula::pow2e;
use slate::{BlockStorage, Index, Position, Reader, Result, Serializable, Storage};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{Cursor, Read, Seek, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::hashtree::HashTree;
use crate::{MemKVS, splitmix64};

pub const MAX_DATA_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub enum NodeKind {
  Leaf { data: Vec<u8> },
  Branch { left: Position, right: Position },
}

/// Node representation in the hash tree
#[derive(Debug, Clone)]
pub struct Node {
  pub position: Position,
  pub index: u64,
  pub hash: Hash,
  pub kind: NodeKind,
}

impl Node {
  pub fn new_leaf(position: u64, index: u64, data: Vec<u8>) -> Self {
    let hash = blake3::hash(&data);
    let leaf = NodeKind::Leaf { data };
    Node { position, index, hash, kind: leaf }
  }

  pub fn new_internal(position: u64, index: u64, hash: Hash, left: Position, right: Position) -> Self {
    let branch = NodeKind::Branch { left, right };
    Node { position, index, hash, kind: branch }
  }

  pub fn is_leaf(&self) -> bool {
    match self.kind {
      NodeKind::Leaf { .. } => true,
      NodeKind::Branch { .. } => false,
    }
  }
}

impl Serializable for Node {
  fn write<W: Write>(&self, w: &mut W) -> slate::Result<usize> {
    // Index (8 bytes)
    w.write_u64::<LittleEndian>(self.index)?;

    // Hash (32 bytes)
    debug_assert_eq!(OUT_LEN, self.hash.as_bytes().len());
    w.write_all(self.hash.as_bytes())?;

    // MetaData (1 byte)
    w.write_u8(if self.is_leaf() { 1 } else { 0 })?;

    let len = match &self.kind {
      NodeKind::Leaf { data } => {
        // Data length and data (if leaf)
        w.write_u32::<LittleEndian>(data.len() as u32)?;
        w.write_all(data)?;
        4 + data.len()
      }
      NodeKind::Branch { left, right } => {
        // Children indices (8 bytes each)
        w.write_u64::<LittleEndian>(*left).unwrap();
        w.write_u64::<LittleEndian>(*right).unwrap();
        8 + 8
      }
    };
    Ok(8 + OUT_LEN + 1 + len)
  }

  fn read<R: Read + Seek>(r: &mut R, position: slate::Position) -> slate::Result<Self> {
    // Index
    let index = r.read_u64::<LittleEndian>()?;

    // Hash
    let mut hash_bytes = [0u8; OUT_LEN];
    r.read_exact(&mut hash_bytes)?;
    let hash = Hash::from(hash_bytes);

    // Metadata
    let is_leaf = r.read_u8()? != 0;

    let kind = if is_leaf {
      // Data
      let data_len = r.read_u32::<LittleEndian>()? as usize;
      let mut data = vec![0u8; data_len];
      r.read_exact(&mut data)?;
      NodeKind::Leaf { data }
    } else {
      // Children
      let left = r.read_u64::<LittleEndian>()?;
      let right = r.read_u64::<LittleEndian>()?;
      NodeKind::Branch { left, right }
    };

    Ok(Node { position, index, hash, kind })
  }
}

struct MetaInfo {
  root: Position,
  height: u8,
}

impl Serializable for MetaInfo {
  fn write<W: Write>(&self, w: &mut W) -> slate::Result<usize> {
    w.write_u64::<LittleEndian>(self.root)?;
    w.write_u8(self.height)?;
    Ok(8 + 8)
  }

  fn read<R: Read + Seek>(r: &mut R, _position: Position) -> slate::Result<Self> {
    let root = r.read_u64::<LittleEndian>()?;
    let height = r.read_u8()?;
    Ok(MetaInfo { root, height })
  }
}

/// 1. 配列インデックス方式
///
/// 完全二分木の性質を利用し、ノードを配列インデックスで管理：
/// インデックス関係:
///
/// - 親ノード i の左の子: 2i + 1
/// - 親ノード i の右の子: 2i + 2  
/// - 子ノード i の親: (i-1)/2
///
/// 3. 階層化方式
///
/// レベル単位でノードをグループ化：
/// [ヘッダ][レベル0][レベル1]...[レベルn][データ領域]
///
/// 各レベル:
/// - ノード数 (4bytes)
/// - ノード配列 (ノード数 × ノードサイズ)
///
/// Binary Hash Tree implementation with file-based storage
pub struct BinaryHashTree<S>
where
  S: Storage<Node>,
{
  storage: S,
  root: Position,
  height: u8,
  cache: Cache, // In-memory cache
}

impl<S> BinaryHashTree<S>
where
  S: Storage<Node>,
{
  fn create<V>(storage: &mut S, h: u8, values: V) -> Result<()>
  where
    V: Fn(u64) -> Vec<u8>,
  {
    debug_assert!(h > 0);
    let (node, position) = storage.first()?;
    debug_assert!(node.is_none());

    // メタ情報の保存 (位置を特定するために空のデータを書き込み)
    let position_metadata = position;
    let metadata = MetaInfo { root: 0, height: 0 };
    let mut buffer = Vec::new();
    metadata.write(&mut buffer)?;
    let meta = Node::new_leaf(position_metadata, 0, buffer);
    let position_root = storage.put(position_metadata, &meta)?;

    // メタ情報の保存
    let metadata = MetaInfo { root: position_root, height: h };
    let mut buffer = Vec::new();
    metadata.write(&mut buffer)?;
    let meta = Node::new_leaf(position_metadata, 0, buffer);
    let position_root2 = storage.put(position_metadata, &meta)?;
    assert_eq!(position_root, position_root2);

    // すべてのノードを書き込み
    Self::create_for_level(storage, position_root, h, 0, values)?;
    Ok(())
  }

  fn create_for_level<V>(storage: &mut S, mut current: Position, h: u8, level: u8, values: V) -> Result<Vec<Node>>
  where
    V: Fn(u64) -> Vec<u8>,
  {
    let offset = pow2e(level);
    let length = pow2e(level);
    let mut nodes = Vec::with_capacity(length as usize);
    for k in 0..length {
      let index = offset + k;
      let node = if level + 1 == h {
        let value = values(k + 1);
        Node::new_leaf(current, index, value)
      } else {
        let mut hasher = Hasher::new();
        hasher.update(&[]);
        Node::new_internal(current, index, hasher.finalize(), u64::MAX, u64::MAX)
      };
      current = storage.put(current, &node)?;
      nodes.push(node);
    }
    if level + 1 < h {
      let subnodes = Self::create_for_level(storage, current, h, level + 1, values)?;
      for (k, node) in nodes.iter_mut().enumerate() {
        let left = subnodes.get(2 * k).unwrap();
        let right = subnodes.get(2 * k + 1).unwrap();
        node.hash = Self::combine(&left.hash, &right.hash);
        node.kind = NodeKind::Branch { left: left.position, right: right.position };
        storage.put(node.position, node)?;
      }
    }
    Ok(nodes)
  }

  fn create_cache(storage: &mut S, height: u8, root: Position, limit: usize) -> Result<Cache> {
    let mut cache = HashMap::with_capacity(limit);
    let mut queue = VecDeque::new();
    let mut reader = storage.reader()?;
    queue.push_back(root);
    'cache_read: for level in 0..=height {
      for _ in 0..pow2e(level) {
        let position = queue.pop_front().unwrap();
        let node = reader.read(position)?;
        if cache.len() + queue.len() < limit
          && let Node { kind: NodeKind::Branch { left, right }, .. } = &node
        {
          queue.push_back(*left);
          queue.push_back(*right);
        }
        cache.insert(position, node);
        if cache.len() == limit || queue.is_empty() {
          break 'cache_read;
        }
      }
    }
    Ok(Cache { cache })
  }

  fn load(&self, reader: &mut Box<dyn Reader<Node>>, position: Position) -> Result<Node> {
    if let Some(node) = self.cache.get(position) { Ok(node.clone()) } else { Ok(reader.read(position)?) }
  }

  fn combine(left: &Hash, right: &Hash) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(left.as_bytes());
    hasher.update(right.as_bytes());
    hasher.finalize()
  }
}

impl BinaryHashTree<BlockStorage<FileDevice>> {
  /// Create a new binary hash tree with file storage
  pub fn from_file<P: AsRef<Path>>(path: P, cache_limit: usize) -> Result<Self> {
    let storage = BlockStorage::from_file(path, false)?;
    Self::new(storage, cache_limit)
  }

  /// Create a new binary hash tree with file storage
  pub fn create_on_file<P, V>(path: P, h: u8, cache_limit: usize, values: V) -> Result<Self>
  where
    P: AsRef<Path>,
    V: Fn(u64) -> Vec<u8>,
  {
    if path.as_ref().exists() {
      fs::remove_file(&path)?;
    }
    let mut storage = BlockStorage::from_file(path, false)?;
    Self::create(&mut storage, h, values)?;
    Self::new(storage, cache_limit)
  }
}

impl BinaryHashTree<MemKVS<Node>> {
  /// Create a new binary hash tree with file storage
  pub fn create_on_memory(h: u8) -> Result<Self> {
    let mut storage = MemKVS::new();
    Self::create(&mut storage, h, |i| splitmix64(i).to_le_bytes().to_vec())?;
    Self::new(storage, 1)
  }

  pub fn create_on_memory_with_kvs(h: u8, kvs: Arc<RwLock<HashMap<Position, Node>>>) -> Result<Self> {
    let mut storage = MemKVS::with_kvs(kvs);
    Self::create(&mut storage, h, |i| splitmix64(i).to_le_bytes().to_vec())?;
    Self::new(storage, 1)
  }
}

impl<S> BinaryHashTree<S>
where
  S: Storage<Node>,
{
  /// Create a new binary hash tree with file storage
  pub fn new(mut storage: S, cache_limit: usize) -> Result<Self> {
    let (metadata, _) = storage.first()?;
    if let Some(Node { kind: NodeKind::Leaf { mut data }, .. }) = metadata {
      let meta = MetaInfo::read(&mut Cursor::new(&mut data), 0)?;
      let root = meta.root;
      let height = meta.height;
      let cache = Self::create_cache(&mut storage, height, root, cache_limit)?;
      Ok(BinaryHashTree { storage, root, height, cache })
    } else {
      panic!()
    }
  }
}

impl<S: Storage<Node>> HashTree for BinaryHashTree<S> {
  type Error = slate::error::Error;

  fn size(&self) -> u64 {
    pow2e(self.height - 1)
  }

  fn get(&mut self, k: u64) -> Result<Option<Vec<u8>>> {
    if k == 0 || k > self.size() {
      Ok(None)
    } else {
      let mut reader = self.storage.reader()?;
      let mut current = self.load(&mut reader, self.root)?;
      loop {
        match &current {
          Node { kind: NodeKind::Branch { left, right }, .. } => {
            let position = if move_left(self.height, &current, k) { *left } else { *right };
            current = self.load(&mut reader, position)?;
          }
          Node { kind: NodeKind::Leaf { data }, .. } => {
            debug_assert_eq!(k, index_to_leaf_number(current.index, self.height), "{}, {}", current.index, self.height);
            debug_assert_eq!(k, index_to_level_position(current.index).1);
            break Ok(Some(data.clone()));
          }
        }
      }
    }
  }
}

/// level, position ≧ 0
fn index_to_level_position(index: u64) -> (u8, u64) {
  debug_assert!(index > 0);
  if index == 1 {
    (0, 1)
  } else {
    let level = (u64::BITS - 1 - index.leading_zeros()) as u8;
    let position = index - (1 << level) + 1;
    (level, position)
  }
}

/// 高さ h ∈ {1,2,...} の完全二分木は、k∈{1,2,...,2^(h-1)} の葉を持つ。したがって、インデックス
/// 1≦i≦2^(h-1) が中間ノードで 2^(h-1)+1≦i≦2^h が葉ノード。
fn index_to_leaf_number(index: u64, height: u8) -> u64 {
  debug_assert!(height > 0);
  debug_assert!(index > 0);
  let leaf_start_index = 1u64 << (height - 1);
  debug_assert!(index >= leaf_start_index, "index {index} is not a leaf node");
  index + 1 - leaf_start_index
}

fn move_left(height: u8, node: &Node, k: Index) -> bool {
  debug_assert!(height > 0);
  debug_assert!(k > 0);
  debug_assert!(!node.is_leaf());
  let (level, position_in_level) = index_to_level_position(node.index);
  let subtree_depth = height - level - 1;
  let leaves_per_subtree = 1 << subtree_depth;
  let first_leaf = (position_in_level - 1) * leaves_per_subtree + 1;
  let boundary = first_leaf + (leaves_per_subtree / 2);
  k < boundary
}

/// A cache that prioritizes the storing of higher-level nodes.
struct Cache {
  cache: HashMap<u64, Node>,
}

impl Cache {
  fn get(&self, position: u64) -> Option<&Node> {
    self.cache.get(&position)
  }
}

#[cfg(test)]
mod test;

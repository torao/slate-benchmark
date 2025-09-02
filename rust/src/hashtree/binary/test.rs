use std::sync::Arc;

use super::*;

#[test]
fn verify_binary_tree() {
  for height in 1..=8 {
    println!("🌲{height}");
    let kvs = Arc::new(RwLock::new(HashMap::new()));
    BinaryHashTree::create_on_memory_with_kvs(height, kvs.clone()).unwrap();

    let mut kvs = kvs.read().unwrap().clone();
    let meta = if let NodeKind::Leaf { data } = &kvs.remove(&1).unwrap().kind {
      MetaInfo::read(&mut Cursor::new(data), 0).unwrap()
    } else {
      panic!()
    };
    assert_eq!(height, meta.height);
    assert_eq!(2, meta.root);

    let mut list = kvs.iter().map(|(pos, node)| (*pos, node.clone())).collect::<Vec<_>>();
    list.sort_by_key(|(_, node)| node.index);
    let mut k = 1;
    for (i, (position, node)) in list.iter().enumerate() {
      print!("  @{position}: [{}] ", node.index);
      assert_eq!(*position, node.position);
      assert_eq!(i as u64 + 1, node.index);
      let (level, pos) = index_to_level_position(node.index);
      match &node.kind {
        NodeKind::Branch { left, right } => {
          println!("👈{} {}👉", kvs.get(left).unwrap().index, kvs.get(right).unwrap().index);
          assert!(level < height);
        }
        NodeKind::Leaf { data } => {
          let bytes: [u8; 8] = data[..8].try_into().unwrap();
          let value = u64::from_le_bytes(bytes);
          println!("🌱 {value}");
          assert_eq!(splitmix64(k), value);
          assert_eq!(index_to_leaf_number(node.index, height), k);
          assert_eq!(level + 1, height);
          assert_eq!(pos, k);
          k += 1;
        }
      }
    }
  }
}

#[test]
fn test_basic_operations() {
  for height in 1..=8 {
    let mut tree = BinaryHashTree::create_on_memory(height).unwrap();
    assert_eq!(pow2e(height - 1), tree.size());

    // Test retrieval
    assert_eq!(tree.get(0).unwrap(), None);
    for k in 1..=tree.size() {
      assert_eq!(tree.get(k).unwrap(), Some(splitmix64(k).to_le_bytes().to_vec()), "{k}");
    }
    assert_eq!(tree.get(tree.size() + 1).unwrap(), None);
  }
}

#[test]
fn verify_level() {
  for (level, position, index) in [
    (0, 1, 1),
    (1, 1, 2),
    (1, 2, 3),
    (2, 1, 4),
    (2, 2, 5),
    (2, 3, 6),
    (2, 4, 7),
    (3, 1, 8),
    (3, 8, 15),
    (4, 1, 16),
    (63, 0x8000000000000000, u64::MAX),
  ] {
    let (lvl, pos) = index_to_level_position(index);
    assert_eq!(level, lvl);
    assert_eq!(position, pos);
  }
}

#[test]
fn verify_move_left() {
  assert!(move_left(2, &inode(1), 1));
  assert!(!move_left(2, &inode(1), 2));
  assert!(move_left(3, &inode(1), 1));
  assert!(move_left(3, &inode(1), 2));
  assert!(!move_left(3, &inode(1), 3));
  assert!(!move_left(3, &inode(1), 4));
  assert!(move_left(3, &inode(2), 1));
  assert!(!move_left(3, &inode(2), 2));
  assert!(move_left(3, &inode(3), 3));
  assert!(!move_left(3, &inode(3), 4));

  for height in 2..u64::BITS as u8 {
    for level in 0..height - 1 {
      assert!(
        move_left(height, &inode(pow2e(level)), 1),
        "move_left({height}, Node{{index:{}}}, 1), level={level}",
        pow2e(level)
      );
      assert!(
        !move_left(height, &inode(2 * pow2e(level) - 1), pow2e(height) / 2),
        "move_left({height}, Node{{index:{}}}, {}), level={level}",
        2 * pow2e(level) - 1,
        pow2e(height) / 2
      );
    }
  }
}

fn inode(index: u64) -> Node {
  let hash = Hash::from_bytes([0u8; OUT_LEN]);
  Node::new_internal(0, index, hash, 0, 0)
}

use std::{fs::remove_file, path::Path};

use criterion::{Criterion, criterion_group, criterion_main};
use slate_benchmark::hashtree::{HashTree as _, binary::BinaryHashTree};

fn bench_binaryhashtree(c: &mut Criterion) {
  c.bench_function("binary-hash-tree", |b| {
    let path = Path::new("bench-binaryhashtree.db");
    let mut tree = BinaryHashTree::create_on_file(path, 10, 10, |i| i.to_le_bytes().to_vec()).unwrap();
    b.iter(|| {
      for i in 0..tree.size() {
        tree.get(i + 1).unwrap();
      }
    });
    if path.exists() {
      remove_file(path).unwrap();
    }
  });
}

criterion_group!(benches, bench_binaryhashtree);
criterion_main!(benches);

use criterion::{criterion_group, criterion_main, Criterion};
use rbmerkle::RedBlackTree;
use winter_crypto::{hashers::Rp64_256, Hasher};

fn h(i: i32) -> <Rp64_256 as Hasher>::Digest {
    Rp64_256::hash(&i.to_be_bytes())
}

fn insert() {
    let mut tree: RedBlackTree<i32, Rp64_256> = RedBlackTree::new();
    let hash = h(0);
    for i in 0..10000 {
        tree.insert(i, hash);
    }
}

fn insert_with_hash() {
    let mut tree: RedBlackTree<i32, Rp64_256> = RedBlackTree::new();
    let hash = h(0);
    for i in 0..10000 {
        tree.insert(i, hash);
    }
    tree.root_hash();
}

fn hash() {
    for i in 0..10000 {
        h(i);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert", |b| b.iter(insert));
    c.bench_function("insert_with_hash", |b| b.iter(insert_with_hash));
    c.bench_function("hash", |b| b.iter(hash));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

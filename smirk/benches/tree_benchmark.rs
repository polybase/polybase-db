use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use smirk::{
    batch::{Batch, Operation},
    smirk, MerkleTree,
};

pub fn insert_benchmark(c: &mut Criterion) {
    let mut rng = ChaChaRng::from_seed([0; 32]);
    let mut nums = vec![0; 1000];
    rng.fill(nums.as_mut_slice());

    c.bench_with_input(
        BenchmarkId::new("insert", "1k random"),
        &nums.as_slice(),
        |bencher, nums| {
            bencher.iter(|| {
                let mut tree = smirk! {};
                for i in *nums {
                    tree.insert(i, i);
                }
                black_box(tree);
            });
        },
    );
}

pub fn collect_benchmark(c: &mut Criterion) {
    let mut rng = ChaChaRng::from_seed([0; 32]);
    let mut nums = vec![0; 1000];
    rng.fill(nums.as_mut_slice());

    c.bench_with_input(
        BenchmarkId::new("collect", "1k random"),
        &nums.as_slice(),
        |bencher, nums| {
            bencher.iter(|| {
                let tree: MerkleTree<_, _> = nums.iter().copied().map(|i| (i, i)).collect();
                black_box(tree);
            });
        },
    );
}

pub fn batch_insert_benchmark(c: &mut Criterion) {
    let mut rng = ChaChaRng::from_seed([0; 32]);
    let mut nums = vec![0; 1000];
    rng.fill(nums.as_mut_slice());

    let batch = Batch::from_operations(nums.into_iter().map(|i| Operation::Insert(i, i)).collect());

    c.bench_with_input(
        BenchmarkId::new("batch insert", "1k random"),
        &batch,
        |bencher, batch| {
            bencher.iter(|| {
                let mut tree = smirk! {};
                tree.apply(batch.clone());
                black_box(tree);
            });
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = insert_benchmark, collect_benchmark, batch_insert_benchmark
}
criterion_main!(benches);

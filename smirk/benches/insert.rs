use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use smirk::smirk;

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

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = insert_benchmark
}
criterion_main!(benches);

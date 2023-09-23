use core::iter::once;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use miden_assembly::Assembler;
use miden_crypto::merkle::InnerNodeInfo;
use miden_processor::{AdviceInputs, MemAdviceProvider, StackInputs};
use miden_prover::{prove, ProofOptions};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use smirk::hash::Digest;

enum Tree {
    Leaf(Digest),
    Node {
        left: Box<Tree>,
        right: Box<Tree>,
        digest: Digest,
    },
}

impl Tree {
    fn digest(&self) -> Digest {
        match self {
            Tree::Leaf(digest) => *digest,
            Tree::Node { digest, .. } => *digest,
        }
    }
    fn to_inner_node_info(&self) -> InnerNodeInfo {
        match self {
            Tree::Leaf(digest) => InnerNodeInfo {
                value: (*digest).into(),
                left: Digest::NULL.into(),
                right: Digest::NULL.into(),
            },
            Tree::Node {
                left,
                right,
                digest,
            } => InnerNodeInfo {
                value: (*digest).into(),
                left: left.digest().into(),
                right: right.digest().into(),
            },
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Tree> + 'a> {
        match self {
            Tree::Leaf(_) => Box::new(once(self)),
            Tree::Node { left, right, .. } => {
                let iter = once(self).chain(left.iter()).chain(right.iter());
                Box::new(iter)
            }
        }
    }
}

/// actually 2^n
fn tree_size_n(rng: &mut impl Rng, n: usize) -> Tree {
    match n {
        0 => Tree::Leaf(random_hash(rng)),
        _ => {
            let left = Box::new(tree_size_n(rng, n - 1));
            let right = Box::new(tree_size_n(rng, n - 1));
            let digest = [left.digest(), right.digest()].iter().collect();

            Tree::Node {
                left,
                right,
                digest,
            }
        }
    }
}

fn random_hash(rng: &mut impl Rng) -> Digest {
    loop {
        let mut bytes = [0; 32];
        rng.fill_bytes(&mut bytes);
        match Digest::from_bytes(bytes) {
            Some(digest) => return digest,
            None => continue,
        }
    }
}

fn merge_trees(tree1: &Tree, tree2: &Tree) {
    let mut advice = AdviceInputs::default();
    advice.extend_merkle_store(tree1.iter().map(Tree::to_inner_node_info));
    advice.extend_merkle_store(tree2.iter().map(Tree::to_inner_node_info));

    let advice = MemAdviceProvider::from(advice);
    let program = Assembler::default()
        .compile("begin mtree_merge end")
        .unwrap();
    let stack = [tree1.digest().to_elements(), tree2.digest().to_elements()]
        .iter()
        .flatten()
        .copied()
        .collect();

    let stack = StackInputs::new(stack);

    let (stack, proof) = prove(&program, stack, advice, ProofOptions::default()).unwrap();

    black_box(stack);
    black_box(proof);
}

macro_rules! tree_merge_bench {
    ($size1:literal, $size2:literal, $name:ident) => {
        fn $name(c: &mut Criterion) {
            let mut rng = ChaChaRng::from_seed([0; 32]);
            let tree1 = tree_size_n(&mut rng, $size1);
            let tree2 = tree_size_n(&mut rng, $size2);
            println!("trees done");

            let input = (tree1, tree2);

            let id = BenchmarkId::new("merge_trees", stringify!($name));

            c.bench_with_input(id, &input, |b, (tree1, tree2)| {
                b.iter(|| merge_trees(tree1, tree2))
            });
        }
    };
}

tree_merge_bench!(10, 10, merge_1k_trees);
tree_merge_bench!(20, 20, merge_1m_trees);
tree_merge_bench!(10, 20, merge_1k_1m_trees);
tree_merge_bench!(10, 27, merge_1k_100m_trees);

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets =
        merge_1k_trees,
        merge_1m_trees,
        merge_1k_1m_trees,
        merge_1k_100m_trees,
}
criterion_main!(benches);

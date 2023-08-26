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
    let mut bytes = [0; 32];
    rng.fill_bytes(&mut bytes);
    Digest::calculate(&bytes)
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

fn merge_1k_tree(c: &mut Criterion) {
    let size = 10;

    let mut rng = ChaChaRng::from_seed([0; 32]);
    let tree1 = tree_size_n(&mut rng, size);
    let tree2 = tree_size_n(&mut rng, size);

    let input = (tree1, tree2);

    let id = BenchmarkId::new("merge_large_trees", size);

    c.bench_with_input(id, &input, |b, (tree1, tree2)| {
        b.iter(|| merge_trees(tree1, tree2))
    });
}

fn merge_1m_tree(c: &mut Criterion) {
    let size = 20;

    let mut rng = ChaChaRng::from_seed([0; 32]);
    let tree1 = tree_size_n(&mut rng, size);
    let tree2 = tree_size_n(&mut rng, size);

    let input = (tree1, tree2);

    let id = BenchmarkId::new("merge_large_trees", size);

    c.bench_with_input(id, &input, |b, (tree1, tree2)| {
        b.iter(|| merge_trees(tree1, tree2))
    });
}

fn merge_1m_1k_tree(c: &mut Criterion) {
    let mut rng = ChaChaRng::from_seed([0; 32]);
    let tree1 = tree_size_n(&mut rng, 10);
    let tree2 = tree_size_n(&mut rng, 20);

    let input = (tree1, tree2);

    let id = BenchmarkId::new("merge_large_trees", 15);

    c.bench_with_input(id, &input, |b, (tree1, tree2)| {
        b.iter(|| merge_trees(tree1, tree2))
    });
}

fn merge_1b_tree(c: &mut Criterion) {
    let size = 30;

    let mut rng = ChaChaRng::from_seed([0; 32]);
    let tree1 = tree_size_n(&mut rng, size);
    let tree2 = tree_size_n(&mut rng, size);

    let input = (tree1, tree2);

    let id = BenchmarkId::new("merge_large_trees", size);

    c.bench_with_input(id, &input, |b, (tree1, tree2)| {
        b.iter(|| merge_trees(tree1, tree2))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = merge_1k_tree, merge_1m_tree, merge_1m_1k_tree, merge_1b_tree
}
criterion_main!(benches);

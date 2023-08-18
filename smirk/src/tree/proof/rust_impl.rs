#![allow(dead_code)]

use crate::hash::Digest;

struct Node {
    left: Option<Digest>,
    right: Option<Digest>,
    this: Digest,
    balance_factor: isize,

}

fn rebalance(node: &mut Node) {

}

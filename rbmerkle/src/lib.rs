#![warn(clippy::unwrap_used, clippy::expect_used)]

use std::fmt::Debug;
use winter_crypto::Hasher;

#[derive(Debug, PartialEq, Clone, Copy)]
enum Color {
    Red,
    Black,
}

impl Color {
    fn is_red(self) -> bool {
        self == Color::Red
    }
    fn is_black(self) -> bool {
        self == Color::Black
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Direction {
    Left,
    Right,
}

impl Direction {
    fn opposite(self) -> Self {
        match self {
            Direction::Left => Direction::Right,
            Direction::Right => Direction::Left,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Node<T, H: Hasher> {
    key: T,
    value: H::Digest,
    color: Color,
    parent: Option<usize>,
    left: Option<usize>,
    right: Option<usize>,
    hash: Option<H::Digest>,
}

impl<T: Ord + Clone + Debug, H: Hasher> Node<T, H> {
    fn new(key: T, value: H::Digest) -> Self {
        Node {
            key,
            value,
            // All new nodes are red
            color: Color::Red,
            parent: None,
            left: None,
            right: None,
            hash: None,
        }
    }
}

// A Red-Black Merkle Tree is a self-balancing binary search tree that supports merkle proofs.
// Each node has an additional attribute called color that can be either red or black. For each node, all
// simple paths from the node to descendant leaves contain the same number of black nodes.
// These rules ensure that the height of the tree remains logarithmic, and the tree remains balanced.
//
// The rules for a Red-Black tree are as follows:
//
// 1. Every node is either red or black.
// 2. The root node is always black.
// 3. Every leaf node (NULL) is black.
// 4. If a node is red, then both its children must be black.
#[derive(Debug)]
pub struct RedBlackTree<T, H: Hasher> {
    root: Option<usize>,
    // TODO: use a vector instead of a hashmap
    nodes: Vec<Node<T, H>>,
}

impl<T: Ord + Copy + Debug, H: Hasher> RedBlackTree<T, H> {
    pub fn new() -> Self {
        RedBlackTree {
            root: None,
            nodes: Vec::new(),
        }
    }

    pub fn has(&self, key: T) -> bool {
        self.find(key).is_some()
    }

    pub fn insert(&mut self, key: T, value: H::Digest) {
        if let Some(id) = self.find(key) {
            self.nodes[id].value = value;
            return;
        }

        // Create the new node
        let new_node = Node::new(key, value);

        // Insert the node into the tree
        self.nodes.push(new_node);
        let id = self.nodes.len() - 1;

        // Find the parent node to insert the new node into
        // Parent node will be None if there is no root node
        let mut parent_id = None;
        let mut parent_key = None;
        let mut node_id = self.root;

        while let Some(n) = self.node(node_id) {
            parent_id = node_id;
            parent_key = Some(n.key);
            // We can unwrap here, as we know the node exists,
            // otherwise the while would have broken
            node_id = if key < n.key { n.left } else { n.right };
        }

        if let Some(pk) = parent_key {
            if key < pk {
                self.set_child(parent_id, Direction::Left, Some(id));
            } else {
                self.set_child(parent_id, Direction::Right, Some(id));
            }

            // Now we have moved the items around, we need to fixup the tree
            // to make sure it is still a valid red-black tree
            self.fixup(Some(id));
        } else {
            self.root = Some(id);
        }

        // Root node must always be black
        self.set_color(self.root, Color::Black);
    }

    pub fn root_hash(&mut self) -> Option<H::Digest> {
        self.hash_subtree(self.root)
    }

    fn hash_subtree(&mut self, id: Option<usize>) -> Option<H::Digest> {
        let node = self.node(id)?;
        let left = node.left;
        let right = node.right;
        let value_hash = node.value;
        let left_hash = self.hash_subtree(left).unwrap_or(H::hash(&[0u8; 7]));
        let right_hash = self.hash_subtree(right).unwrap_or(H::hash(&[0u8; 7]));
        let child_hash = H::merge(&[left_hash, right_hash]);
        let hash = H::merge(&[value_hash, child_hash]);
        // let hash = H::hash_elements(&[value_hash.as_bytes(), left_hash.as_bytes(), right_hash.as_bytes()]);
        self.set_hash(id, Some(hash));
        Some(hash)
    }

    // Rotate the tree in the given direction
    fn rotate(&mut self, id: Option<usize>, direction: Direction) {
        let new_root_id;
        let parent_id = self.node(id).and_then(|n| n.parent);
        let is_left_child = self.is_child(id, Direction::Left);

        match direction {
            // Rotate 5 left:
            //   5      =>     7
            //  / \           / \
            // 3   7         5   8
            //    / \       / \
            //   6   8     3  6
            // 1. Take 7 as new root
            // 2. Take 6 as new right child of 5
            // 3. Take 5 as new left child of 7
            // 4. Replace 5 with 7
            // Swap the new_root and existing node
            Direction::Left => {
                new_root_id = self.node(id).and_then(|n| n.right);
                let new_root_left_id = self.node(new_root_id).and_then(|n| n.left);
                self.set_child(id, Direction::Right, new_root_left_id);
                self.set_child(new_root_id, Direction::Left, id);
            }
            // Rotate 5 right:
            //     5      =>     3
            //    / \           / \
            //   3   7         1   5
            //  / \               / \
            // 1   4             4   7
            // 1. Take 3 as new root
            // 2. Take 4 as new left child of 5
            // 3. Take 5 as new right child of 3
            // 4. Replace 5 with 3
            Direction::Right => {
                new_root_id = self.node(id).and_then(|n| n.left);
                let new_root_right_id = self.node(new_root_id).and_then(|n| n.right);
                self.set_child(id, Direction::Left, new_root_right_id);
                self.set_child(new_root_id, Direction::Right, id);
            }
        };

        // Move new_root_id to child of parent
        if is_left_child {
            self.set_child(parent_id, Direction::Left, new_root_id);
        } else {
            self.set_child(parent_id, Direction::Right, new_root_id)
        }
    }

    fn fixup(&mut self, id: Option<usize>) {
        let node = self.node(id);
        let parent_id = node.and_then(|n| n.parent);
        let parent = self.node(parent_id);

        // Check if parent is explicitly red (i.e. exists and has parent)
        if !parent.map(|n| n.color.is_red()).unwrap_or(false) {
            return;
        }

        let grandparent_id = parent.and_then(|n| n.parent);
        let uncle_id = parent.and_then(|parent| {
            let gp = self.node(parent.parent);
            if self.is_child(parent_id, Direction::Left) {
                gp.and_then(|gp| gp.right)
            } else {
                gp.and_then(|gp| gp.left)
            }
        });

        if self.is_red(uncle_id) {
            // Case 1: Uncle is red, recolour grandparent, parent and uncle
            self.set_color(parent_id, Color::Black);
            self.set_color(uncle_id, Color::Black);
            self.set_color(grandparent_id, Color::Red);
            self.fixup(grandparent_id);
        } else {
            let parent_is_left = self.is_child(parent_id, Direction::Left);
            let parent_is_right = !parent_is_left;

            // Case 2: Uncle is black and parent direction is different to child direction (arrow)
            if parent_is_left && self.is_child(id, Direction::Right) {
                self.rotate(parent_id, Direction::Left);
                self.fixup(parent_id);
            } else if parent_is_right && self.is_child(id, Direction::Left) {
                self.rotate(parent_id, Direction::Right);
                self.fixup(parent_id);
            } else {
                // Case 3: Uncle is black and parent is right child (straight line)
                if self.is_child(id, Direction::Left) {
                    self.rotate(grandparent_id, Direction::Right);
                } else {
                    self.rotate(grandparent_id, Direction::Left);
                }
                self.set_color(parent_id, Color::Black);
                self.set_color(grandparent_id, Color::Red);
            }
        }
    }

    pub fn delete(&mut self, key: T) {
        // Find id of node
        let id = self.find(key);

        // Check if the key to be deleted exists
        if id.is_none() {
            return;
        }

        let z_id = id;
        let z = self.node(id);
        let z_right = z.and_then(|n| n.right);
        let z_left = z.and_then(|n| n.left);
        let z_color = z.map(|n| n.color).unwrap_or(Color::Black);

        // Save the original colour
        let mut original_color = z_color;
        let x_id;
        let y_id;

        if z_left.is_none() {
            x_id = z_right;
            self.delete_transplant(z_id, z_right)
        } else if z_right.is_none() {
            x_id = z_left;
            self.delete_transplant(z_id, z_left)
        } else {
            y_id = self.minimum(z_right);

            let y = self.node(y_id);
            let y_right = y.and_then(|n| n.right);
            let y_left = y.and_then(|n| n.left);
            original_color = y.map(|n| n.color).unwrap_or(Color::Black);
            x_id = y_right;

            if y.and_then(|n| n.parent) == z_id {
                self.set_parent(x_id, y_id);
            } else {
                self.delete_transplant(y_id, y_right);
                self.set_child(y_id, Direction::Right, z_right);
                self.set_parent(y_right, y_id);
            }

            self.delete_transplant(z_id, y_id);
            self.set_child(y_id, Direction::Left, z_left);
            self.set_parent(y_left, y_id);
            self.set_color(y_id, z_color);
        }

        if original_color.is_black() {
            self.delete_fixup(x_id);
        }

        // Remove node from vector, we can unwrap because we check
        // if is_none() above
        #[allow(clippy::unwrap_used)]
        self.delete_swap(id.unwrap())
    }

    // Remove the deleted node from the vector, first we have to move
    // it to the end of the vector, so it doesn't mess up the indexing
    // for all other nodes
    fn delete_swap(&mut self, id: usize) {
        // Get node in last position
        let last_index = self.nodes.len() - 1;
        let last_node = &self.nodes[last_index];
        let parent = last_node.parent;
        let left = last_node.left;
        let right = last_node.right;

        self.set_parent(left, Some(id));
        self.set_parent(right, Some(id));

        if self.is_child(Some(last_index), Direction::Left) {
            self.set_child(parent, Direction::Left, Some(id));
        } else {
            self.set_child(parent, Direction::Right, Some(id));
        }

        // Update the root if needed
        if self.root == Some(last_index) {
            self.root = Some(id);
        }

        // Actually swap the position
        self.nodes.swap(id, last_index);

        // Remove the node to be deleted now it is at
        // the end of the vector
        self.nodes.pop();
    }

    // Transplant O(1) the subtree rooted at u with the subtree rooted at v
    fn delete_transplant(&mut self, u_id: Option<usize>, v_id: Option<usize>) {
        let u = self.node(u_id);
        let u_parent_id = u.and_then(|n| n.parent);
        let u_parent = self.node(u_parent_id);

        // If u is root, set v as root
        if u_parent_id.is_none() {
            self.root = v_id;
        }
        // If u is left child of parent, set v as left child
        else if u_id == u_parent.and_then(|n| n.left) {
            self.set_child(u_parent_id, Direction::Left, v_id);
        }
        // If u is right child of parent, set v as right child
        else {
            self.set_child(u_parent_id, Direction::Right, v_id);
        }
        self.set_parent(v_id, u_parent_id);
    }

    fn delete_fixup(&mut self, id: Option<usize>) {
        let mut x_id = id;
        while x_id.is_some() {
            if self.root == x_id || self.is_red(x_id) {
                break;
            }

            // Get the direction
            let direction = if self.is_child(x_id, Direction::Left) {
                Direction::Left
            } else {
                Direction::Right
            };

            let mut w_id = self.parent_child(x_id, direction.opposite());

            // Type 1
            if self.is_red(w_id) {
                let w_parent = self.parent(w_id);
                self.set_color(w_id, Color::Black);
                self.set_color(w_parent, Color::Red);
                self.rotate(w_parent, direction);
                w_id = self.parent_child(x_id, direction.opposite())
            }

            // Type 2
            if !self.is_red(self.child(w_id, Direction::Left))
                && !self.is_red(self.child(w_id, Direction::Right))
            {
                self.set_color(w_id, Color::Red);
                x_id = self.parent(x_id);
            } else {
                // Type 3
                if !self.is_red(self.child(w_id, direction.opposite())) {
                    self.set_color(self.child(w_id, direction), Color::Black);
                    self.set_color(w_id, Color::Red);
                    self.rotate(w_id, direction.opposite());
                    w_id = self.parent_child(x_id, direction.opposite());
                }

                // Type 4
                self.set_color(w_id, self.color(self.parent(x_id)));
                self.set_color(self.parent(x_id), Color::Black);
                self.set_color(self.child(w_id, direction.opposite()), Color::Black);
                self.rotate(self.parent(x_id), direction);
                x_id = self.root;
            }
        }

        self.set_color(x_id, Color::Black);
    }

    fn find(&self, key: T) -> Option<usize> {
        let mut current_id = self.root;
        while let Some(n) = self.node(current_id) {
            if n.key == key {
                return current_id;
            }
            current_id = if key < n.key { n.left } else { n.right };
        }
        current_id
    }

    // Find the minimum node O(log n) in the subtree rooted at id
    fn minimum(&self, id: Option<usize>) -> Option<usize> {
        let mut current_id = id;
        while let Some(node) = self.node(current_id) {
            if node.left.is_none() {
                break;
            }
            current_id = node.left;
        }
        current_id
    }

    // Debug helpers
    pub fn print(&self) {
        self.print_node(self.root, 0);
    }

    fn print_node(&self, id: Option<usize>, depth: usize) {
        let indent = "     ".repeat(depth);
        if id.is_some() {
            #[allow(clippy::unwrap_used)]
            let node = self.node(id).unwrap();
            println!(
                "{indent} key={:?}, left={:?}, right={:?} parent={:?}, hash={:?}",
                node.key, node.left, node.right, node.parent, node.hash
            );
            self.print_node(node.left, depth + 1);
            self.print_node(node.right, depth + 1);
        } else {
            println!("{indent} None");
        }
    }

    // Node helpers
    fn node(&self, id: Option<usize>) -> Option<&Node<T, H>> {
        id.map(|id| &self.nodes[id])
    }

    fn node_mut(&mut self, id: Option<usize>) -> Option<&mut Node<T, H>> {
        id.map(|id| &mut self.nodes[id])
    }

    fn parent(&self, id: Option<usize>) -> Option<usize> {
        self.node(id).and_then(|node| node.parent)
    }

    fn child(&self, id: Option<usize>, direction: Direction) -> Option<usize> {
        self.node(id).and_then(|node| match direction {
            Direction::Left => node.left,
            Direction::Right => node.right,
        })
    }

    fn parent_child(&self, id: Option<usize>, direction: Direction) -> Option<usize> {
        self.parent(id)
            .and_then(|n| self.node(Some(n)))
            .and_then(|parent| match direction {
                Direction::Left => parent.left,
                Direction::Right => parent.right,
            })
    }

    fn is_child(&self, id: Option<usize>, direction: Direction) -> bool {
        self.parent(id)
            .and_then(|p_id| self.node(Some(p_id)))
            .and_then(|parent| {
                if direction == Direction::Left {
                    parent.left
                } else {
                    parent.right
                }
            })
            .map(|child_id| Some(child_id) == id)
            .unwrap_or(false)
    }

    fn set_child(
        &mut self,
        parent_id: Option<usize>,
        direction: Direction,
        child_id: Option<usize>,
    ) {
        if let Some(node) = self.node_mut(parent_id) {
            if direction == Direction::Left {
                node.left = child_id;
            } else {
                node.right = child_id;
            }
        } else {
            self.root = child_id;
        }
        if let Some(child) = self.node_mut(child_id) {
            child.parent = parent_id;
        }
    }

    fn set_parent(&mut self, id: Option<usize>, parent: Option<usize>) {
        if let Some(node) = self.node_mut(id) {
            node.parent = parent;
        }
    }

    fn color(&self, id: Option<usize>) -> Color {
        self.node(id).map(|n| n.color).unwrap_or(Color::Black)
    }

    fn set_color(&mut self, id: Option<usize>, color: Color) {
        if let Some(node) = self.node_mut(id) {
            node.color = color;
        }
    }

    fn set_hash(&mut self, id: Option<usize>, hash: Option<H::Digest>) {
        if let Some(node) = self.node_mut(id) {
            node.hash = hash
        }
    }

    fn is_red(&self, id: Option<usize>) -> bool {
        self.node(id).map(|n| n.color.is_red()).unwrap_or(false)
    }
}

impl<T: Ord + Copy + std::hash::Hash + Debug, H: Hasher> Default for RedBlackTree<T, H> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winter_crypto::hashers::Rp64_256;

    fn assert_balanced(tree: &RedBlackTree<i32, Rp64_256>) {
        // Check equal number of nodes
        let mut red = 0;
        let mut black = 0;
        for i in &tree.nodes {
            if i.color.is_red() {
                red += 1;
            } else {
                black += 1;
            }
        }
        if red - black > 1 || black - red > 1 {
            panic!("Unbalanced tree: Red: {red}, Black: {black}");
        }
    }

    fn assert_black_root(tree: &RedBlackTree<i32, Rp64_256>) {
        if tree.root.is_some() && tree.color(tree.root).is_red() {
            panic!("Root is red");
        }
    }

    fn get_node(tree: &RedBlackTree<i32, Rp64_256>, key: i32) -> Option<&Node<i32, Rp64_256>> {
        tree.node(tree.find(key))
    }

    fn h(i: i32) -> <Rp64_256 as Hasher>::Digest {
        let mut bytes = [0u8; 7];
        let i_bytes = i.to_le_bytes();
        bytes[0..4].copy_from_slice(&i_bytes);
        winter_crypto::hashers::Rp64_256::hash(&bytes)
    }

    #[derive(Debug)]
    struct TestNode {
        left: Option<usize>,
        right: Option<usize>,
        parent: Option<usize>,
    }

    fn assert_node(node: &Node<i32, Rp64_256>, partial: TestNode) {
        let key = node.key;
        let TestNode {
            left,
            right,
            parent,
        } = partial;
        assert_eq!(node.left, left, "key: {key} / left = {left:?}");
        assert_eq!(node.right, right, "key: {key} / right != {right:?}");
        assert_eq!(node.parent, parent, "key: {key} / parent != {parent:?}");
    }

    #[test]
    fn test_insert_root() {
        let mut tree: RedBlackTree<i32, Rp64_256> = RedBlackTree::new();
        tree.insert(1, h(0));
        assert!(tree.has(1));
        assert_eq!(tree.root.unwrap(), 0);
    }

    #[test]
    fn test_set_child() {
        let mut tree: RedBlackTree<i32, Rp64_256> = RedBlackTree::new();
        tree.root = Some(0);
        tree.nodes.push(Node {
            key: 0,
            value: h(0),
            left: None,
            right: None,
            parent: None,
            color: Color::Black,
            hash: None,
        });
        tree.nodes.push(Node {
            key: 1,
            value: h(0),
            left: None,
            right: None,
            parent: None,
            color: Color::Black,
            hash: None,
        });
        tree.set_child(Some(0), Direction::Left, Some(1));
        assert_eq!(tree.nodes[0].left, Some(1));
        assert_eq!(tree.nodes[1].parent, Some(0));

        tree.set_child(None, Direction::Left, Some(1));
        assert_eq!(tree.root, Some(1));
        assert_eq!(tree.nodes[1].parent, None);
    }

    #[test]
    fn test_rotate() {
        let mut tree: RedBlackTree<i32, Rp64_256> = RedBlackTree::new();
        tree.root = Some(0);
        tree.nodes.push(Node {
            key: 3,
            value: h(0),
            left: Some(2),
            right: None,
            parent: None,
            color: Color::Black,
            hash: None,
        });
        tree.nodes.push(Node {
            key: 2,
            value: h(0),
            left: Some(1),
            right: None,
            parent: Some(3),
            color: Color::Red,
            hash: None,
        });
        tree.nodes.push(Node {
            key: 1,
            value: h(0),
            left: None,
            right: None,
            parent: Some(2),
            color: Color::Red,
            hash: None,
        });
        tree.rotate(Some(0), Direction::Right);

        assert_node(
            get_node(&tree, 3).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: Some(2),
            },
        );
    }

    #[test]
    fn test_insert() {
        let mut tree = RedBlackTree::new();

        tree.insert(3, h(0));
        tree.insert(2, h(0));
        tree.insert(1, h(0));

        tree.insert(12, h(0));
        tree.insert(32, h(0));
        tree.insert(123, h(0));
        tree.insert(14, h(0));
        tree.insert(20, h(0));
        tree.insert(6, h(0));
        tree.insert(41, h(0));
        tree.insert(122, h(0));

        // Test that the tree has the correct items
        assert!(tree.has(1));
        assert!(tree.has(2));
        assert!(tree.has(3));
        assert!(tree.has(12));

        // Duplicate item is not added
        // TODO: should update if duplicate
        tree.insert(41, h(0));
        assert!(tree.nodes.len() == 11);

        // Check equal number of nodes
        assert_balanced(&tree);
        assert_black_root(&tree);

        // Check structure
        assert_eq!(tree.root, tree.find(12));
        // Root
        assert_node(
            get_node(&tree, 12).unwrap(),
            TestNode {
                left: tree.find(2),
                right: tree.find(32),
                parent: None,
            },
        );
        // Left
        assert_node(
            get_node(&tree, 2).unwrap(),
            TestNode {
                left: tree.find(1),
                right: tree.find(3),
                parent: tree.find(12),
            },
        );
        assert_node(
            get_node(&tree, 1).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(2),
            },
        );
        assert_node(
            get_node(&tree, 3).unwrap(),
            TestNode {
                left: None,
                right: tree.find(6),
                parent: tree.find(2),
            },
        );
        assert_node(
            get_node(&tree, 6).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(3),
            },
        );
        // Right
        assert_node(
            get_node(&tree, 32).unwrap(),
            TestNode {
                left: tree.find(14),
                right: tree.find(122),
                parent: tree.find(12),
            },
        );
        assert_node(
            get_node(&tree, 14).unwrap(),
            TestNode {
                left: None,
                right: tree.find(20),
                parent: tree.find(32),
            },
        );
        assert_node(
            get_node(&tree, 20).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(14),
            },
        );
        assert_node(
            get_node(&tree, 122).unwrap(),
            TestNode {
                left: tree.find(41),
                right: tree.find(123),
                parent: tree.find(32),
            },
        );
        assert_node(
            get_node(&tree, 123).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(122),
            },
        );

        // tree.print();
    }

    #[test]
    fn test_delete() {
        let mut tree = RedBlackTree::new();

        tree.insert(12, h(0));
        tree.insert(23, h(0));
        tree.insert(1, h(0));
        tree.insert(8, h(0));
        tree.insert(9, h(0));
        tree.insert(10, h(0));
        tree.insert(13, h(0));
        tree.insert(15, h(0));

        // Delete root
        tree.delete(12);

        assert_eq!(tree.nodes.len(), 7);
        assert_balanced(&tree);
        assert_black_root(&tree);

        assert_eq!(tree.root, tree.find(13));

        // Root
        assert_node(
            get_node(&tree, 13).unwrap(),
            TestNode {
                left: tree.find(8),
                right: tree.find(15),
                parent: None,
            },
        );
        // Left
        assert_node(
            get_node(&tree, 8).unwrap(),
            TestNode {
                left: tree.find(1),
                right: tree.find(9),
                parent: tree.find(13),
            },
        );
        assert_node(
            get_node(&tree, 1).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(8),
            },
        );
        assert_node(
            get_node(&tree, 9).unwrap(),
            TestNode {
                left: None,
                right: tree.find(10),
                parent: tree.find(8),
            },
        );
        assert_node(
            get_node(&tree, 10).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(9),
            },
        );
        // Right
        assert_node(
            get_node(&tree, 15).unwrap(),
            TestNode {
                left: None,
                right: tree.find(23),
                parent: tree.find(13),
            },
        );
        assert_node(
            get_node(&tree, 23).unwrap(),
            TestNode {
                left: None,
                right: None,
                parent: tree.find(15),
            },
        );

        // tree.print();
    }

    #[test]
    fn test_hash() {
        let mut tree: RedBlackTree<i32, Rp64_256> = RedBlackTree::new();
        tree.insert(1, h(0));
        tree.insert(2, h(0));
        tree.insert(3, h(0));

        assert!(tree.root_hash().is_some());

        assert!(get_node(&tree, 1).and_then(|t| t.hash).is_some());
        assert!(get_node(&tree, 2).and_then(|t| t.hash).is_some());
        assert!(get_node(&tree, 3).and_then(|t| t.hash).is_some());

        // println!("{:?}", tree.root_hash().unwrap().as_bytes())
    }
}

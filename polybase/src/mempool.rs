use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use std::vec;
use tokio::sync::oneshot;

struct MempoolTxn<V, C> {
    txn: V,
    senders: Option<Vec<oneshot::Sender<()>>>,
    changes: Vec<C>,
}

pub struct Mempool<K, V, L, C> {
    state: Arc<Mutex<MempoolState<K, V, L, C>>>,
}

pub struct MempoolState<K, V, L, C> {
    txns: HashMap<K, MempoolTxn<V, C>>,
    pool: VecDeque<K>,
    leased: HashMap<L, HashSet<K>>,
}

impl<K, V, L, C> Mempool<K, V, L, C>
where
    K: Eq + PartialEq + Hash + Clone,
    V: Clone,
    L: Eq + PartialEq + Hash + Clone,
    C: Eq + PartialEq + Hash + Clone,
{
    pub fn new() -> Self {
        Mempool {
            state: Arc::new(Mutex::new(MempoolState {
                txns: HashMap::new(),
                pool: VecDeque::new(),
                leased: HashMap::new(),
            })),
        }
    }

    /// Add a transaction to the mempool, only adds key/txn if the key
    /// doesn't already exist in the mempool. This is used when other nodes
    /// send us a txn they have received from a client
    pub fn add(&self, key: K, txn: V, changes: Vec<C>) {
        self._add(key, txn, changes, None);
    }

    // #[cfg(test)]
    // pub fn has(&self, key: &K) -> bool {
    //     self.txns.lock().contains_key(key)
    // }

    /// Add a transaction to the mempool and wait for it to be committed. This will only
    /// be called where the txn is directly submitted to this node from a client
    pub async fn add_wait(&self, key: K, txn: V, changes: Vec<C>) {
        let (tx, rx) = oneshot::channel();
        self._add(key, txn, changes, Some(vec![tx]));
        let _ = rx.await;
    }

    /// Internal add function, used by both add and add_wait
    fn _add(&self, key: K, txn: V, changes: Vec<C>, tx: Option<Vec<oneshot::Sender<()>>>) {
        let mut state = self.state.lock();

        state.txns.entry(key.clone()).or_insert(MempoolTxn {
            txn,
            senders: tx,
            changes,
        });

        // Add the key to the pool
        state.pool.push_back(key);
    }

    /// Commit a given transaction with key, removing it from the mempool
    /// and resolving any waiting futures (from add_txn_wait)
    pub fn commit(&self, lease: L, keys: Vec<&K>) {
        let mut state = self.state.lock();

        for key in keys {
            if let Some(mem_txn) = state.txns.remove(key) {
                if let Some(senders) = mem_txn.senders {
                    for sender in senders {
                        let _ = sender.send(());
                    }
                }
            }

            if let Some(lease) = state.leased.get_mut(&lease) {
                lease.remove(key);
            }

            if let Some(pos) = state.pool.iter().position(|x| x == key) {
                // TODO: this is very inefficient, we should find a better way to do this
                state.pool.remove(pos);
            }
        }

        // Drop lock before calling free with lock
        drop(state);

        // Free the leaseed items that are not committed
        self.free(lease);
    }

    /// Free a set of leased txns, these txns will now be unlocked and
    /// available for other leases
    fn free(&self, lease: L) {
        let mut state = self.state.lock();

        // Get the keys in the lease, and push them back into the pool, putting
        // them first so they are highest priority
        state
            .leased
            .remove(&lease)
            .unwrap_or_default()
            .into_iter()
            .for_each(|k| state.pool.push_front(k));
    }

    /// Lease a set of txns, these txns will now be locked until the lease
    /// is committed
    pub fn lease(&self, lease: L, max_count: usize) -> Vec<(K, V)> {
        let mut state = self.state.lock();
        let mut txns = vec![];
        let mut discard = vec![];
        let mut conflict_check = HashSet::new();

        while let Some(key) = state.pool.pop_front() {
            #[allow(clippy::expect_used)]
            let changes = state
                .txns
                .get(&key)
                .expect("key not found in txns")
                .changes
                .clone();

            // A change key has already been included in a previously added txn
            if changes.iter().any(|c| conflict_check.contains(c)) {
                discard.push(key);
                continue;
            }

            state
                .leased
                .entry(lease.clone())
                .or_insert(HashSet::new())
                .insert(key.clone());

            conflict_check.extend(changes);

            #[allow(clippy::unwrap_used)]
            txns.push((key.clone(), state.txns.get(&key).unwrap().txn.clone()));

            // If we have reached the max count, break
            if txns.len() >= max_count {
                break;
            }
        }

        // Return the discarded keys to the pool
        state.pool.extend(discard);

        txns
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_add_txn() {
        let mempool: Mempool<String, u32, usize, usize> = Mempool::new();
        mempool.add("key1".to_string(), 42, vec![]);

        {
            let state = mempool.state.lock();
            assert_eq!(state.txns.len(), 1);
            assert_eq!(state.txns.get("key1").unwrap().txn, 42);
        }

        mempool.add("key1".to_string(), 24, vec![]);

        {
            let state = mempool.state.lock();
            assert_eq!(state.txns.len(), 1);
            assert_eq!(state.txns.get("key1").unwrap().txn, 42);
        }

        {
            let state = mempool.state.lock();
            assert_eq!(state.txns.len(), 1);
            assert_eq!(state.txns.get("key1").unwrap().txn, 42);
        }
    }

    #[test]
    fn test_add_txn_wait() {
        let mempool: Arc<Mempool<String, u32, usize, usize>> = Arc::new(Mempool::new());
        let rt = Runtime::new().unwrap();

        let mempool2 = mempool.clone();
        rt.spawn(async move {
            mempool2.add_wait("key1".to_string(), 42, vec![]).await;
        });

        sleep(Duration::from_millis(100));

        {
            let state = mempool.state.lock();
            assert_eq!(state.txns.len(), 1);
            assert_eq!(state.txns.get("key1").unwrap().txn, 42);
        }
    }

    #[test]
    fn test_commit_txn() {
        let mempool: Mempool<&'static str, u32, usize, usize> = Mempool::new();
        mempool.add("key1", 42, vec![]);
        mempool.add("key2", 24, vec![]);

        mempool.commit(1, vec![&"key1"]);

        let state = mempool.state.lock();
        assert_eq!(state.txns.len(), 1);
        assert_eq!(state.pool.len(), 1);
        assert!(state.txns.get(&"key1").is_none());
        assert_eq!(state.txns.get("key2").unwrap().txn, 24);
    }

    #[test]
    fn test_lease() {
        let mempool: Mempool<String, u32, usize, usize> = Mempool::new();
        mempool.add("key1".to_string(), 42, vec![]);
        mempool.add("key2".to_string(), 24, vec![]);
        mempool.add("key3".to_string(), 15, vec![]);

        let batch = mempool.lease(2, 2);
        assert_eq!(batch.len(), 2);

        {
            let state = mempool.state.lock();
            assert_eq!(state.pool.len(), 1);
        }

        mempool.commit(2, vec![&"key1".to_string()]);

        let batch = mempool.lease(2, 2);
        assert_eq!(batch.len(), 2);

        // assert_eq!(batch[1], ("key2".to_string(), 24));
        // assert_eq!(batch[0], ("key1".to_string(), 42));
    }

    #[test]
    fn test_lease_with_duplicate_changes() {
        let mempool: Mempool<String, u32, usize, usize> = Mempool::new();
        mempool.add("key1".to_string(), 42, vec![1, 2, 3]);
        mempool.add("key2".to_string(), 24, vec![3, 4, 5]);
        mempool.add("key3".to_string(), 15, vec![6, 7, 8]);

        let batch = mempool.lease(2, 3);
        assert_eq!(batch.len(), 2);

        {
            let state = mempool.state.lock();
            assert_eq!(state.pool.len(), 1);
        }
    }
}

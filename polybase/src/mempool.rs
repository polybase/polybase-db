use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::vec;
use tokio::sync::oneshot;

struct MempoolTxn<V> {
    txn: V,
    senders: Option<Vec<oneshot::Sender<()>>>,
}

pub struct Mempool<K, V> {
    txns: Arc<Mutex<HashMap<K, MempoolTxn<V>>>>,
}

impl<K: Eq + PartialEq + Hash + Clone, V: Clone> Mempool<K, V> {
    pub fn new() -> Self {
        Mempool {
            txns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a transaction to the mempool, only adds key/txn if the key
    /// doesn't already exist in the mempool
    pub fn add(&mut self, key: K, txn: V) {
        let mut txns = self.txns.lock();
        txns.entry(key).or_insert(MempoolTxn { txn, senders: None });
    }

    pub fn has(&self, key: &K) -> bool {
        self.txns.lock().contains_key(key)
    }

    /// Add a transaction to the mempool and wait for it to be committed
    pub async fn add_wait(&self, key: K, txn: V) {
        let (tx, rx) = oneshot::channel();
        {
            let mut txns = self.txns.lock();
            txns.insert(
                key,
                MempoolTxn {
                    txn,
                    senders: Some(vec![tx]),
                },
            );
        }
        let _ = rx.await;
    }

    /// Commit a given transaction with key, removing it from the mempool
    /// and resolving any waiting futures (from add_txn_wait)
    pub fn commit(&self, key: K) {
        let mut txns = self.txns.lock();
        if let Some(mem_txn) = txns.remove(&key) {
            if let Some(senders) = mem_txn.senders {
                for sender in senders {
                    let _ = sender.send(());
                }
            }
        }
    }

    /// Get a batch of txns to be committed, in order they were received.
    pub fn get_batch(&self, max_count: usize) -> Vec<(K, V)> {
        let txns = self.txns.lock();
        txns.iter()
            .take(max_count)
            .map(|(key, mempool_txn)| (key.clone(), mempool_txn.txn.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_add_txn() {
        let mut mempool: Mempool<String, u32> = Mempool::new();
        mempool.add("key1".to_string(), 42);

        {
            let txns = mempool.txns.lock();
            assert_eq!(txns.len(), 1);
            assert_eq!(txns.get("key1").unwrap().txn, 42);
        }

        mempool.add("key1".to_string(), 24);

        {
            let txns = mempool.txns.lock();
            assert_eq!(txns.len(), 1);
            assert_eq!(txns.get("key1").unwrap().txn, 42);
        }
    }

    #[test]
    fn test_add_txn_wait() {
        let mempool: Arc<Mempool<String, u32>> = Arc::new(Mempool::new());
        let rt = Runtime::new().unwrap();

        let mempool2 = mempool.clone();
        rt.spawn(async move {
            mempool2.add_wait("key1".to_string(), 42).await;
        });

        {
            let txns = mempool.txns.lock();
            assert_eq!(txns.len(), 1);
            assert_eq!(txns.get("key1").unwrap().txn, 42);
        }
    }

    #[test]
    fn test_commit_txn() {
        let mut mempool: Mempool<String, u32> = Mempool::new();
        mempool.add("key1".to_string(), 42);
        mempool.add("key2".to_string(), 24);

        mempool.commit("key1".to_string());

        let txns = mempool.txns.lock();
        assert_eq!(txns.len(), 1);
        assert!(txns.get("key1").is_none());
        assert_eq!(txns.get("key2").unwrap().txn, 24);
    }

    #[test]
    fn test_get_txn_batch() {
        let mut mempool: Mempool<String, u32> = Mempool::new();
        mempool.add("key1".to_string(), 42);
        mempool.add("key2".to_string(), 24);
        mempool.add("key3".to_string(), 15);

        let batch = mempool.get_batch(2);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], ("key1".to_string(), 42));
        assert_eq!(batch[1], ("key2".to_string(), 24));
    }
}

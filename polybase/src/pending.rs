use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex};
use std::hash::Hash;

type Result<T> = std::result::Result<T, PendingQueueError>;

pub enum PendingQueueError {
    KeyExists,
}

pub struct PendingQueue<K, V> {
   state: Mutex<PendingState<K, V>>
}

struct PendingState<K, V> {
    pending: VecDeque<Value<K, V>>,
    pending_lock: HashSet<K>,
}

pub struct Value <K, V> {
    pub key: K,
    pub value: V,
}

impl <K: Eq + PartialEq + Hash + Clone, V> PendingQueue <K, V> {
    pub fn new() -> Self {
        Self{
            state: Mutex::new(
                PendingState {
                    pending: VecDeque::new(),
                    pending_lock: HashSet::new(),
                }
            )
        }
    }

    pub fn insert(&self, key: K, value: V) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.pending_lock.contains(&key) {
            return Err(PendingQueueError::KeyExists);
        }
        state.pending_lock.insert(key.clone());
        state.pending.push_back(Value { key, value });
        Ok(())
    }

    pub fn pop(&self) -> Option<Value<K, V>>  {
        let mut state = self.state.lock().unwrap();
        let value = state.pending.pop_front()?;
        state.pending_lock.remove(&value.key);
        Some(value)
    }
}

impl <K: Eq + PartialEq + Hash + Clone, V> Default for PendingQueue <K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl <K: Eq + PartialEq  + Hash + Clone, V> Iterator for PendingQueue <K, V> {
    type Item = Value<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

use std::collections::{HashSet, HashMap};
use std::sync::{RwLock, Arc};
use winter_crypto::{Hasher, Digest};
use winter_crypto::{hashers::Rp64_256};

use gateway::{Gateway, Change};
use indexer::{Indexer, RecordValue};
use rbmerkle::RedBlackTree;

pub struct Db {
    pending: RwLock<Vec<Change>>,
    pending_lock: RwLock<HashSet<[u8; 32]>>,
    gateway: Gateway,
    rollup: RwLock<RedBlackTree<[u8; 32], Rp64_256>>,

    // TODO: indexer should be replaced with a kv store
    indexer: Arc<Indexer>,
}

impl Db {
    pub fn new(indexer: Arc<Indexer>) -> Self {
        Self{
            pending: RwLock::new(Vec::new()),
            pending_lock: RwLock::new(HashSet::new()),
            gateway: gateway::initialize(),
            indexer,
            rollup: RwLock::new(RedBlackTree::<[u8; 32], Rp64_256>::new()),
        }
    }

    pub fn commit(&self, commit_until_key: [u8; 32]) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        // TODO: If there is a commit to collection metadata, we should ignore other changes?

        // Cachce collections
        for change in self.pending.write().unwrap().drain(..) {
            let key;

            // Insert into indexer
            match change {
                Change::Create { record, collection_id, record_id } => {
                    key = get_key(&collection_id, &record_id);
                    self.set(collection_id, record_id, record)?;
                },
                Change::Update { record, collection_id, record_id } => {
                    key = get_key(&collection_id, &record_id);
                    self.set(collection_id, record_id, record)?;
                },
                Change::Delete { record_id, collection_id } => {
                    key = get_key(&collection_id, &record_id);
                    // todo!()
                },
            }

            // Commit up until this point
            if key == commit_until_key {
                break;
            }
        }

        // Remove all entries from the pending_lock
        self.pending_lock.write().unwrap().clear();

        // Get the new rollup hash
        // self.update_hash();

        Ok(())
    }
    
    fn set(&self, collection_id: String, record_id: String, record: HashMap<String, RecordValue>) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let collection = self.indexer.collection(collection_id.clone())?;
        collection.set(record_id.clone(), &record, None)?;
        let key = get_key(&collection_id, &record_id);
        let b = bincode::serialize(&record)?;
        let hash = Rp64_256::hash(&b);
        let mut rollup = self.rollup.write().unwrap();
        rollup.insert(key, hash);
        Ok(())
    }

    pub fn call(&self, 
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<RecordValue>,
        auth: Option<&indexer::AuthUser>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let indexer = Arc::clone(&self.indexer);
        let changes = match self.gateway.call(
            &indexer,
            collection_id,
            function_name,
            record_id,
            args,
            auth,
        ) {
            Ok(changes) => changes,
            Err(e) => return Err(e),
        };

        let mut pending = self.pending.write().unwrap();
        let mut pending_lock = self.pending_lock.write().unwrap();

        // First we cache the result, as it will be committed later
        for change in changes {
            let (collection_id, record_id) = change.get_path();
            let k = get_key(collection_id, record_id);

            if pending_lock.contains(&k) {
                return Err("Record already exists in cache".into());
            }

            pending_lock.insert(k);
            pending.push(change);
        }

        // TODO: Return a handle that returns on next commit

        Ok(())
    }
}

fn get_key(namespace: &String, id: &String) -> [u8; 32] {
    let b = [namespace.as_bytes(), id.as_bytes()].concat();
    Rp64_256::hash(&b).as_bytes()
}
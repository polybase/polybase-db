use std::collections::HashMap;
use std::sync::{RwLock, Arc};
use winter_crypto::{hashers::Rp64_256};


use gateway::{Gateway, Change};
use indexer::{Indexer, RecordValue};
use rbmerkle::RedBlackTree;
use crate::config::Config;


pub struct Db {
    cache: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    // TODO: indexer should be replaced with a kv store
    indexer: Arc<Indexer>,
    gateway: Gateway,
    rollup: RedBlackTree<[u8; 10],Rp64_256>,
    hash: Option<Rp64_256>
}


impl Db {
    pub fn new(indexer: Arc<Indexer>, config: &Config) -> Self {
        Self{
            cache: RwLock::new(HashMap::new()),
            gateway: gateway::initialize(),
            indexer,
            rollup: RedBlackTree::<[u8; 10],Rp64_256>::new(),
            hash: None,
        }
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

        for change in changes {
            match change {
                Change::Create {
                    collection_id,
                    record_id,
                    record,
                } => {
                    let collection = indexer.collection(collection_id)?;
                    collection.set(record_id, &record, auth)?;
                }
                Change::Update {
                    collection_id,
                    record_id,
                    record,
                } => {
                    let collection = indexer.collection(collection_id)?;
                    collection.set(record_id, &record, auth)?;
                }
                Change::Delete { record_id: _ } => todo!(),
            }
        }

        Ok(())
    }
}
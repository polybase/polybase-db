use crate::{
    adaptor::{self, IndexerAdaptor},
    cursor,
    list_query::ListQuery,
    where_query::{self, WhereQuery},
};
use futures::stream::{FuturesUnordered, StreamExt};
use schema::{
    directive::DirectiveKind,
    field_path::FieldPath,
    index::IndexField,
    publickey::PublicKey,
    record::{ForeignRecordReference, RecordReference, RecordRoot, Reference},
    Schema,
};
use std::{
    borrow::Cow,
    collections::HashSet,
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    time::SystemTime,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("adaptor error: {0}")]
    Adaptor(#[from] adaptor::Error),

    #[error("user error: {0}")]
    User(#[from] UserError),
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("unauthorized read")]
    UnauthorizedRead,

    #[error("collection not found")]
    CollectionNotFound { id: String },

    #[error("invalid cursor, before and after cannot be used together")]
    InvalidCursorBeforeAndAfterSpecified,
}

pub struct Indexer<A: IndexerAdaptor> {
    adaptor: A,
    commit_store: Arc<RwLock<Vec<IndexerChange>>>,
}

pub enum IndexerChange {
    Set,
    SetCollection,
    Create,
    Delete,
    Update,
}

impl<A: IndexerAdaptor> Indexer<A> {
    pub fn new(adaptor: A) -> Self {
        Self {
            adaptor,
            commit_store: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn commit(&self) -> Result<()> {
        // let mut commit_store = self.commit_store.write().unwrap();
        // let commits = mem::replace(&mut *commit_store, vec![]);
        // Ok(self.adaptor.commit().await?)
        Ok(())
    }

    pub async fn set(
        &self,
        collection_id: &str,
        record_id: &str,
        value: &RecordRoot,
    ) -> Result<()> {
        let mut commit_store = self.commit_store.write().unwrap();
        commit_store.push(IndexerChange::Set);
        Ok(())
    }

    pub async fn delete(&self, collection_id: &str, record_id: &str) -> Result<()> {
        let mut commit_store = self.commit_store.write().unwrap();
        commit_store.push(IndexerChange::Delete);
        Ok(())
    }

    pub async fn get(
        &self,
        collection_id: &str,
        record_id: &str,
        public_key: Option<&PublicKey>,
    ) -> Result<Option<RecordRoot>> {
        let record = match self.adaptor.get(collection_id, record_id).await? {
            Some(record) => record,
            None => return Ok(None),
        };

        let schema = self.get_schema_required(collection_id).await?;

        if !self
            .verify_read(collection_id, &schema, &record, public_key)
            .await
        {
            return Err(UserError::UnauthorizedRead)?;
        }

        Ok(Some(record))
    }

    pub async fn get_without_auth_check(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<RecordRoot>> {
        let record = match self.adaptor.get(collection_id, record_id).await? {
            Some(record) => record,
            None => return Ok(None),
        };

        Ok(Some(record))
    }

    pub async fn list(
        &self,
        collection_id: &str,
        query: ListQuery<'_>,
        public_key: Option<&PublicKey>,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = RecordRoot> + '_ + Send>>> {
        let schema = self.get_schema_required(collection_id).await?;

        if !self
            .verify_list(collection_id, &schema, &query.where_query, public_key)
            .await
        {
            return Err(UserError::UnauthorizedRead)?;
        };

        let ListQuery {
            limit,
            where_query,
            order_by,
            cursor_after,
            cursor_before,
        } = query;

        let mut where_query = where_query.clone();

        match (cursor_before, cursor_after) {
            (Some(cursor_before), None) => {
                where_query.apply_cursor(cursor_before, cursor::CursorDirection::Before, order_by)
            }
            (None, Some(cursor_after)) => {
                where_query.apply_cursor(cursor_after, cursor::CursorDirection::After, order_by)
            }
            (Some(_), Some(_)) => {
                return Err(UserError::InvalidCursorBeforeAndAfterSpecified)?;
            }
            (None, None) => {}
        }

        Ok(self
            .adaptor
            .list(collection_id, limit, where_query, order_by)
            .await?)
    }

    pub async fn last_record_update(
        &self,
        collection_id: &str,
        record_id: &str,
    ) -> Result<Option<SystemTime>> {
        Ok(self
            .adaptor
            .last_record_update(collection_id, record_id)
            .await?)
    }

    pub async fn last_collection_update(&self, collection_id: &str) -> Result<Option<SystemTime>> {
        Ok(self.adaptor.last_collection_update(collection_id).await?)
    }

    pub async fn set_system_key(&self, key: &str, data: &RecordRoot) -> Result<()> {
        Ok(self.adaptor.set_system_key(key, data).await?)
    }

    pub async fn get_system_key(&self, key: &str) -> Result<Option<RecordRoot>> {
        Ok(self.adaptor.get_system_key(key).await?)
    }

    /// Verify a list query is valid
    pub async fn verify_list<'a>(
        &self,
        collection_id: &str,
        schema: &Schema,
        where_query: &WhereQuery<'a>,
        public_key: Option<&PublicKey>,
    ) -> bool {
        // Convert where query to a record, so we can verify it
        let record = where_query.to_record_root();

        self.verify_read(collection_id, schema, &record, public_key)
            .await
    }

    /// Verify public_key is allowed to call method on record, we don't check record parameters
    /// here, as they will automatically be checked when they are fetched using the normal read
    /// rules.
    pub async fn verify_call(
        &self,
        collection_id: &str,
        method: &str,
        schema: &Schema,
        record: &RecordRoot,
        public_key: Option<&PublicKey>,
    ) -> bool {
        // Always allow call if schema allows any
        if schema.call_all {
            return true;
        }

        // If no public key and not call all, deny call
        let Some(public_key) = public_key else {
            return false;
        };

        // Check for matching public keys in record
        if schema.authorise_method_with_public_key(method, record, public_key) {
            return true;
        }

        // Otherwise, get method references
        let refs = schema.find_method_references(method, record);

        self.verify_references(collection_id, schema, public_key, refs)
            .await
    }

    /// Verify user can read a give record
    pub async fn verify_read(
        &self,
        collection_id: &str,
        schema: &Schema,
        record: &RecordRoot,
        public_key: Option<&PublicKey>,
    ) -> bool {
        // Always allow read if schema allows any
        if schema.read_all {
            return true;
        }

        // If no public key and not read all, deny read
        let public_key = match public_key {
            Some(pk) => pk,
            None => return false,
        };

        // Otherwise get read permissions
        self.verify_directives(
            collection_id,
            &[DirectiveKind::Delegate, DirectiveKind::Read],
            schema,
            record,
            public_key,
        )
        .await
    }

    #[async_recursion::async_recursion]
    pub async fn verify_directives(
        &self,
        collection_id: &str,
        directives: &[DirectiveKind],
        schema: &Schema,
        record: &RecordRoot,
        public_key: &PublicKey,
    ) -> bool {
        // Check for matching public keys in record
        if schema.authorise_directives_with_public_key(directives, record, public_key) {
            return true;
        }

        // Otherwise, get references
        let refs = schema.find_directive_references(directives, record);

        // Create a future for each reference (recursive lookup)
        self.verify_references(collection_id, schema, public_key, refs)
            .await
    }

    pub async fn verify_references<'a>(
        &self,
        collection_id: &str,
        schema: &Schema,
        public_key: &PublicKey,
        refs: impl Iterator<Item = (FieldPath, Reference<'a>)>,
    ) -> bool {
        // Create a future for each reference (recursive lookup)
        let mut futures = FuturesUnordered::new();
        for (_, ref reference) in refs {
            let (collection_id, schema, record_id) = match reference {
                Reference::Record(RecordReference { id }) => {
                    (collection_id, Cow::Borrowed(schema), id.as_str())
                }
                Reference::ForeignRecord(ForeignRecordReference {
                    ref collection_id,
                    ref id,
                }) => match self.adaptor.get_schema(collection_id).await {
                    Ok(Some(schema)) => (collection_id.as_str(), Cow::Owned(schema), id.as_str()),
                    _ => continue,
                },
            };

            // Get record, ignore if record deleted / missing
            let record = match self.adaptor.get(collection_id, record_id).await {
                Ok(Some(record)) => record,
                _ => continue,
            };

            futures.push(async move {
                self.verify_directives(
                    collection_id,
                    // When we recurse, we only look for delegates
                    &[DirectiveKind::Delegate],
                    &schema,
                    &record,
                    public_key,
                )
                .await
            });
        }

        // Check the reults of the futures, find the first one that returns true
        while let Some(result) = futures.next().await {
            if result {
                return true;
            }
        }

        false
    }

    /// Use this instead of self.adaptor.get_schema to error when schema is missing, i.e.
    /// collection does not exist
    pub async fn get_schema_required(&self, collection_id: &str) -> Result<Schema> {
        match self.adaptor.get_schema(collection_id).await? {
            Some(schema) => Ok(schema),
            None => Err(UserError::CollectionNotFound {
                id: collection_id.to_string(),
            })?,
        }
    }
}

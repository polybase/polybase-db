use std::time::SystemTime;

use crate::{publickey::PublicKey, record::RecordRoot};
use serde::{Deserialize, Serialize};

/// The generic collection functionality
#[async_trait::async_trait]
pub trait Collection<'a> {
    type Error;
    type Key;
    type Value;
    type ListQuery;
    type Cursor;

    async fn get_without_auth_check(&self, id: String) -> Result<Option<RecordRoot>, Self::Error>;

    async fn get(
        &self,
        id: String,
        user: Option<&AuthUser>,
    ) -> Result<Option<RecordRoot>, Self::Error>;

    async fn get_record_metadata(
        &self,
        record_id: &str,
    ) -> Result<Option<RecordMetadata>, Self::Error>;

    async fn list(
        &'a self,
        list_query: Self::ListQuery,
        user: &'a Option<&'a AuthUser>,
    ) -> Result<
        Box<dyn futures::Stream<Item = Result<(Self::Cursor, RecordRoot), Self::Error>> + 'a>,
        Self::Error,
    >;

    async fn get_metadata(&self) -> Result<Option<CollectionMetadata>, Self::Error>;
}

pub struct CollectionMetadata {
    pub last_record_updated_at: SystemTime,
}

pub struct RecordMetadata {
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUser {
    pub public_key: PublicKey,
}

impl AuthUser {
    pub fn new(public_key: PublicKey) -> Self {
        Self { public_key }
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

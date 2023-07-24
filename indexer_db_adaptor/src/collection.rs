use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::{
    db::Database,
    publickey::PublicKey,
    record::{self, PathFinder, RecordRoot, RecordValue},
};
use async_recursion::async_recursion;
use base64::Engine;
use futures::StreamExt;
use once_cell::sync::Lazy;
use polylang::stableast;
use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

static COLLECTION_COLLECTION_RECORD: Lazy<crate::record::RecordRoot> = Lazy::new(|| {
    let mut hm = HashMap::new();

    hm.insert(
        "id".to_string(),
        RecordValue::String("Collection".to_string()),
    );

    let code = r#"
@public
collection Collection {
    id: string;
    name?: string;
    lastRecordUpdated?: string;
    code?: string;
    ast?: string;
    publicKey?: PublicKey;

    @index(publicKey);
    @index([lastRecordUpdated, desc]);

    constructor (id: string, code: string) {
        this.id = id;
        this.code = code;
        this.ast = parse(code, id);
        if (ctx.publicKey) this.publicKey = ctx.publicKey;
    }

    updateCode (code: string) {
        if (this.publicKey != ctx.publicKey) {
            throw error('invalid owner');
        }
        this.code = code;
        this.ast = parse(code, this.id);
    }
}
"#;

    hm.insert(
        "code".to_string(),
        // The replaces are for clients <=0.3.23
        RecordValue::String(code.replace("@public", "").replace("PublicKey", "string")),
    );

    let mut program = None;
    #[allow(clippy::unwrap_used)]
    let (_, stable_ast) = polylang::parse(code, "", &mut program).unwrap();
    hm.insert(
        "ast".to_string(),
        #[allow(clippy::unwrap_used)]
        RecordValue::String(serde_json::to_string(&stable_ast).unwrap()),
    );

    hm
});

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

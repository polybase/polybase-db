// use bincode::{deserialize, serialize};
use indexer_db_adaptor::auth_user::AuthUser;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice as deserialize, to_vec as serialize};
use sha3::{Digest, Sha3_256};

pub type Result<T> = std::result::Result<T, CallTxnError>;

#[derive(Debug, thiserror::Error)]
pub enum CallTxnError {
    #[error("serialize error")]
    SerializerError(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallTxn {
    pub collection_id: String,
    pub function_name: String,
    pub record_id: String,
    pub args: Vec<serde_json::Value>,
    pub auth: Option<AuthUser>,
}

impl CallTxn {
    pub fn new(
        collection_id: String,
        function_name: &str,
        record_id: String,
        args: Vec<serde_json::Value>,
        auth: Option<AuthUser>,
    ) -> Self {
        CallTxn {
            collection_id,
            function_name: function_name.to_string(),
            record_id,
            args,
            auth,
        }
    }

    pub fn hash(&self) -> Result<[u8; 32]> {
        let bytes = self.serialize()?;
        let mut hasher = Sha3_256::new();
        hasher.update(bytes);
        let result = hasher.finalize();
        Ok(result.into())
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(serialize(&self)?)
    }

    pub fn deserialize(value: &[u8]) -> Result<Self> {
        Ok(deserialize(value)?)
    }
}

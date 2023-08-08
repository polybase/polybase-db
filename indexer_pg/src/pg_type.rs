use schema::types::{PrimitiveType, Type};
use std::fmt::{self, Display, Formatter};

pub enum PgType {
    Text,
    Float,
    Boolean,
    Json,
    Bytes,
}

impl PgType {
    pub fn default(&self) -> String {
        match &self {
            PgType::Text => "''".to_string(),
            PgType::Float => "0.0".to_string(),
            PgType::Boolean => "false".to_string(),
            PgType::Json => "'{}'".to_string(),
            PgType::Bytes => "'\\x'".to_string(),
        }
    }
}

impl Display for PgType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PgType::Text => write!(f, "TEXT"),
            PgType::Float => write!(f, "FLOAT"),
            PgType::Boolean => write!(f, "BOOLEAN"),
            PgType::Bytes => write!(f, "BYTEA"),
            PgType::Json => write!(f, "JSON"),
        }
    }
}

pub fn schema_type_to_pg_type(value: &Type) -> PgType {
    match value {
        Type::Primitive(PrimitiveType::String) => PgType::Text,
        Type::Primitive(PrimitiveType::Number) => PgType::Float,
        Type::Primitive(PrimitiveType::Boolean) => PgType::Boolean,
        Type::Primitive(PrimitiveType::Bytes) => PgType::Bytes,
        Type::Array(_) => PgType::Json,
        Type::Map(_) => PgType::Json,
        Type::Object(_) => PgType::Json,
        Type::Record => PgType::Json,
        Type::ForeignRecord(_) => PgType::Json,
        Type::PublicKey => PgType::Json,
        Type::Unknown => PgType::Json,
    }
}

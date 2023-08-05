use crate::error::Result;
use polylang::stableast;
use schema::{
    publickey,
    record::{ForeignRecordReference, RecordReference, RecordRoot, RecordValue},
    types::{Array, Map, PrimitiveType, Type},
    util::normalize_name,
    Schema,
};
use sqlx::{postgres::PgRow, Column, Row};
use std::collections::HashMap;

#[derive(Debug, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct CollectionRecordRow {
    pub id: String,
    pub name: String,
    pub ast: String,
    pub code: String,
    pub public_key: Option<serde_json::Value>,
}

pub fn pg_row_to_record_value(row: PgRow, schema: &Schema) -> RecordRoot {
    let mut record: HashMap<String, RecordValue> = HashMap::new();

    let fields = schema.properties.iter();

    let columns = row.columns();
    let mut name_to_index = std::collections::HashMap::new();

    for (index, column) in columns.iter().enumerate() {
        name_to_index.insert(column.name().to_string(), index);
    }

    for prop in fields {
        let name = prop.name().to_string();
        let index = match name_to_index.get(&name) {
            Some(index) => index,
            None => {
                // TODO: if required field, then use default value
                record.insert(name, RecordValue::Null);
                continue;
            }
        };

        let record_value: Option<RecordValue> = match prop.type_ {
            Type::Primitive(PrimitiveType::String) => row
                .try_get::<Option<String>, _>(&index)
                .unwrap_or(None)
                .map(RecordValue::String),
            Type::Primitive(PrimitiveType::Number) => row
                .try_get::<Option<f64>, _>(&index)
                .unwrap_or(None)
                .map(RecordValue::Number),
            Type::Primitive(PrimitiveType::Boolean) => row
                .try_get::<Option<bool>, _>(&index)
                .unwrap_or(None)
                .map(RecordValue::Boolean),
            Type::Primitive(PrimitiveType::Bytes) => row
                .try_get::<Option<Vec<u8>>, _>(&index)
                .unwrap_or(None)
                .map(RecordValue::Bytes),
            Type::Array(Array { .. }) => row
                .try_get::<Option<serde_json::Value>, _>(&index)
                .unwrap_or(None)
                .map(convert_serde_json_value_to_record_value),
            Type::Map(Map { .. }) => row
                .try_get::<Option<serde_json::Value>, _>(&index)
                .unwrap_or(None)
                .map(convert_serde_json_value_to_record_value),
            Type::PublicKey => row
                .try_get::<Option<serde_json::Value>, _>(&index)
                .unwrap_or(None)
                .and_then(convert_serde_json_value_to_public_key),
            Type::Record => row
                .try_get::<Option<serde_json::Value>, _>(&index)
                .unwrap_or(None)
                .and_then(convert_serde_json_value_to_record_ref),
            Type::ForeignRecord(_) => row
                .try_get::<Option<serde_json::Value>, _>(&index)
                .unwrap_or(None)
                .and_then(convert_serde_json_value_to_foreign_record_ref),
            Type::Object(_) => row
                .try_get::<Option<serde_json::Value>, _>(&index)
                .unwrap_or(None)
                .map(convert_serde_json_value_to_record_value),
            Type::Unknown => None,
        };

        if let Some(record_value) = record_value {
            record.insert(name, record_value);
        } else {
            // TODO: if required field, then use default value
            record.insert(name, RecordValue::Null);
        }
    }

    record
}

fn convert_serde_json_value_to_record_value(value: serde_json::Value) -> RecordValue {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_f64() {
                RecordValue::Number(value)
            } else {
                RecordValue::Null
            }
        }
        serde_json::Value::Bool(value) => RecordValue::Boolean(value),
        serde_json::Value::String(value) => RecordValue::String(value),
        serde_json::Value::Null => RecordValue::Null,
        serde_json::Value::Object(map) => {
            let mut new_map = HashMap::new();
            for (key, value) in map {
                new_map.insert(key, convert_serde_json_value_to_record_value(value));
            }
            RecordValue::Map(new_map)
        }
        serde_json::Value::Array(vec) => {
            let new_vec = vec
                .into_iter()
                .map(convert_serde_json_value_to_record_value)
                .collect();
            RecordValue::Array(new_vec)
        }
    }
}

fn convert_serde_json_value_to_public_key(value: serde_json::Value) -> Option<RecordValue> {
    match value {
        serde_json::Value::Object(_) => Some(RecordValue::PublicKey(
            publickey::PublicKey::try_from(value).ok()?,
        )),
        _ => None,
    }
}

fn convert_serde_json_value_to_record_ref(value: serde_json::Value) -> Option<RecordValue> {
    match value {
        serde_json::Value::Object(o) => o.get("id").and_then(|id| match id {
            serde_json::Value::String(id) => Some(RecordValue::RecordReference(RecordReference {
                id: id.to_string(),
            })),
            _ => None,
        }),
        _ => None,
    }
}

fn convert_serde_json_value_to_foreign_record_ref(value: serde_json::Value) -> Option<RecordValue> {
    match value {
        serde_json::Value::Object(o) => {
            let id = o.get("id")?;
            let collection_id = o.get("collection_id")?;
            match (id, collection_id) {
                (serde_json::Value::String(id), serde_json::Value::String(collection_id)) => Some(
                    RecordValue::ForeignRecordReference(ForeignRecordReference {
                        id: id.to_string(),
                        collection_id: collection_id.to_string(),
                    }),
                ),
                _ => None,
            }
        }
        _ => None,
    }
}

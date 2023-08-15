use crate::field_path::FieldPath;
use crate::index_value::IndexValue;
use crate::property::Property;
use crate::publickey;
use crate::schema::Schema;
use crate::types::{ForeignRecord, PrimitiveType, Type};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{hash_map, HashMap},
};

pub type Result<T> = std::result::Result<T, RecordError>;

// TODO: review if we still need all these errors
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    UserError(#[from] RecordUserError),

    #[error("record ID must be a string")]
    RecordIDMustBeAString,

    #[error("invalid boolean byte {b}")]
    InvalidBooleanByte { b: u8 },

    #[error("invalid type prefix {b}")]
    InvalidTypePrefix { b: u8 },

    #[error("failed to convert number to f64")]
    FailedToConvertNumberToF64,

    #[error("failed to convert f64 ({f:?}) to serde number")]
    FailedToConvertF64ToSerdeNumber { f: f64 },

    #[error("record does not have an ID field")]
    RecordIdNotFound,

    #[error("unknown type")]
    UnknownType,

    #[error(transparent)]
    PublicKeyError(#[from] publickey::PublicKeyError),

    #[error("try from int error")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error("try from int error")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("try from slice error")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),

    #[error("from utf8 error")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("base64 decode error")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("serde_json error")]
    SerdeJSONError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RecordUserError {
    #[error("record is missing field {field:?}")]
    MissingField { field: String },

    #[error("record root should be an object, got: {got}")]
    RecordRootShouldBeAnObject { got: serde_json::Value },

    #[error("value at field \"{}\" does not match the schema type, expected type: {expected_type}, got value: {value}", .field.as_deref().unwrap_or("unknown"))]
    InvalidFieldValueType {
        value: serde_json::Value,
        expected_type: String,
        // TODO: we're not populating this, fix me
        field: Option<String>,
    },

    #[error("record reference missing \"{field}\" field")]
    RecordReferenceMissingField { field: String },

    #[error("record reference has invalid field type")]
    RecordReferenceInvalidType { field: String },

    #[error(
        "foreign record reference has incorrect collection id, expected: \"{expected}\", got: \"{got}\""
    )]
    ForeignRecordReferenceHasWrongCollectionId { expected: String, got: String },

    #[error("unexpected fields: {}", .fields.join(", "))]
    UnexpectedFields { fields: Vec<String> },
}

// TODO: RecordRoot should be a RecordValue of type ObjectValue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde()]
pub struct RecordRoot(pub HashMap<String, RecordValue>);

impl RecordRoot {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn id(&self) -> Result<&str> {
        match self.get("id") {
            Some(rv) => match rv {
                RecordValue::String(record_id) => Ok(record_id),
                _ => Err(RecordError::RecordIDMustBeAString),
            },
            None => Err(RecordError::RecordIdNotFound),
        }
    }

    // TODO: this should validate against the schema, do we need this, given
    // we do the conversion based on schema?
    pub fn validate(&self, schema: &Schema) -> Result<()> {
        // Check we have a valid id
        self.id()?;

        let mut keys = self
            .0
            .keys()
            .map(|k| (k.as_str(), true))
            .collect::<HashMap<_, _>>();

        for prop in schema.properties.iter() {
            if let Some(val) = self.0.get(prop.name()) {
                keys.remove(prop.name());
                if prop.required {
                    return Err(RecordUserError::MissingField {
                        field: prop.name().to_string(),
                    })?;
                }
                val.validate_prop(prop)?;
            }
        }

        // Check for extra fields
        if !keys.is_empty() {
            return Err(RecordUserError::UnexpectedFields {
                fields: keys.keys().map(|k| k.to_string()).collect(),
            })?;
        }

        Ok(())
    }

    pub fn insert(&mut self, field: String, value: RecordValue) {
        self.0.insert(field, value);
    }

    // TODO: handle array here too (see fill_path)
    pub fn insert_path(&mut self, path: &FieldPath, value: RecordValue) {
        if path.len() == 1 {
            self.insert(path.name().to_string(), value);
            return;
        }

        self.fill_path(path);

        // We should always have a value here due to fill path
        if let Some(RecordValue::Map(map)) = self.get_path_mut(&path.parent()) {
            map.insert(path.name().to_string(), value);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &RecordValue)> {
        self.0.iter()
    }

    pub fn remove(&mut self, field: &str) -> Option<RecordValue> {
        self.0.remove(field)
    }

    pub fn get(&self, field: &str) -> Option<&RecordValue> {
        self.0.get(field)
    }

    pub fn entry(
        &mut self,
        field: String,
    ) -> std::collections::hash_map::Entry<'_, String, RecordValue> {
        self.0.entry(field)
    }

    // TODO: we should handle Array here too
    pub fn fill_path(&mut self, path: &FieldPath) {
        for part in path.iter().take(path.len() - 1) {
            self.entry(part.to_string())
                .or_insert_with(|| RecordValue::Map(HashMap::new()));
        }
    }

    pub fn get_path_mut(&mut self, field: &FieldPath) -> Option<&mut RecordValue> {
        let mut iter = field.iter();
        let mut val = self.0.get_mut(iter.next()?)?;
        for path in iter {
            match val {
                RecordValue::Array(array) => {
                    let index = path.parse::<usize>().ok()?;
                    val = array.get_mut(index)?;
                }

                RecordValue::Map(map) => {
                    val = map.get_mut(path)?;
                }
                _ => return None,
            }
        }
        Some(val)
    }

    pub fn get_path(&self, field: &FieldPath) -> Option<&RecordValue> {
        let mut iter = field.iter();
        let mut val = self.0.get(iter.next()?)?;
        for path in iter {
            match val {
                RecordValue::Array(array) => {
                    let index = path.parse::<usize>().ok()?;
                    val = array.get(index)?;
                }

                RecordValue::Map(map) => {
                    val = map.get(path)?;
                }
                _ => return None,
            }
        }
        Some(val)
    }

    pub fn try_from_json(schema: &Schema, value: serde_json::Value, force: bool) -> Result<Self> {
        let mut map = HashMap::new();

        // Check root must be an object
        let serde_json::Value::Object(mut value) = value else {
            return Err(RecordUserError::RecordRootShouldBeAnObject { got: value }.into());
        };

        // TODO: should we check for unexpected fields?
        for prop in schema.properties.iter() {
            let Some((name, value)) = value.remove_entry(prop.path.name())
        else {
            // TODO: do we need to check required on sub fields?
            if prop.required {
                if force {
                    // Insert default for the type
                    map.insert(prop.path.name().to_string(), RecordValue::default_from_type(&prop.type_));
                    continue;
                }

                return Err(RecordUserError::MissingField { field: prop.path.to_string() })?;
            } else {
                continue;
            }
        };

            let value = RecordValue::try_from_json_prop(prop, value, force)?;
            map.insert(name, value);
        }

        // TODO: should we validate here?

        Ok(RecordRoot(map))
    }
}

impl Default for RecordRoot {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for RecordRoot {
    type Item = (String, RecordValue);
    type IntoIter = hash_map::IntoIter<String, RecordValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Converts JSON to RootRecord, also validates that the structure is correct
pub fn json_to_record(
    schema: &Schema,
    value: serde_json::Value,
    force: bool,
) -> Result<RecordRoot> {
    RecordRoot::try_from_json(schema, value, force)
}

pub fn record_to_json(value: RecordRoot) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (field, value) in value {
        map.insert(field, value.into());
    }

    serde_json::Value::Object(map)
}

pub fn foreign_record_to_json(value: RecordRoot, collection_id: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (field, value) in value {
        map.insert(field, value.into());
    }

    map.insert("__collectionId".to_string(), collection_id.into());

    serde_json::Value::Object(map)
}

// TODO: should we not have a Object type? Or allow Map key to be
// more than just String
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum RecordValue {
    Number(f64),
    Boolean(bool),
    Null,
    String(String),
    PublicKey(crate::publickey::PublicKey),
    RecordReference(RecordReference),
    ForeignRecordReference(ForeignRecordReference),
    Bytes(Vec<u8>),
    Map(HashMap<String, RecordValue>),
    Array(Vec<RecordValue>),
}

impl RecordValue {
    pub fn cast(self, type_: &Type, path: &FieldPath) -> Result<Self> {
        // No casting needed
        if self.is_type(type_) {
            return Ok(self);
        }

        let v = match type_ {
            // String
            Type::Primitive(PrimitiveType::String) => match self {
                RecordValue::Number(n) => Ok(RecordValue::String(n.to_string())),
                RecordValue::Boolean(b) => Ok(RecordValue::String(b.to_string())),
                RecordValue::Array(a) => Ok(RecordValue::String(serde_json::to_string(&a)?)),
                RecordValue::Map(b) => Ok(RecordValue::String(serde_json::to_string(&b)?)),
                RecordValue::PublicKey(pk) => Ok(RecordValue::String(pk.to_hex()?)),
                RecordValue::Bytes(bytes) => Ok(RecordValue::String(
                    base64::engine::general_purpose::STANDARD.encode(bytes),
                )),
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // Number
            Type::Primitive(PrimitiveType::Number) => match self {
                RecordValue::String(s) => Ok(RecordValue::Number(s.parse::<f64>()?)),
                RecordValue::Boolean(b) => Ok(RecordValue::Number(if b { 1.0 } else { 0.0 })),
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // Boolean
            Type::Primitive(PrimitiveType::Boolean) => match self {
                RecordValue::Number(n) => Ok(RecordValue::Boolean(n == 0.0)),
                RecordValue::String(s) => Ok(RecordValue::Boolean(s == "true")),
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // Bytes
            Type::Primitive(PrimitiveType::Bytes) => match self {
                RecordValue::String(s) => Ok(RecordValue::Bytes(
                    base64::engine::general_purpose::STANDARD.decode(s.as_bytes())?,
                )),
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // PublicKey
            Type::PublicKey => match self {
                RecordValue::String(s) => {
                    Ok(RecordValue::PublicKey(publickey::PublicKey::from_hex(&s)?))
                }
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // Array
            Type::Array(a) => match self {
                RecordValue::Array(array) => {
                    let mut v = Vec::with_capacity(array.len());
                    for (i, value) in array.into_iter().enumerate() {
                        v.push(value.cast(&a.value, &path.append(format!("[{}]", i)))?);
                    }
                    Ok(RecordValue::Array(v))
                }
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // Map
            Type::Map(m) => match self {
                RecordValue::Map(map) => {
                    let mut v = HashMap::with_capacity(map.len());
                    for (key, value) in map {
                        v.insert(key.clone(), value.cast(&m.value, &path.append(key))?);
                    }
                    Ok(RecordValue::Map(v))
                }
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            // Object
            Type::Object(m) => match self {
                RecordValue::Map(mut map) => {
                    let mut v = HashMap::with_capacity(map.len());
                    for p in m.fields.iter() {
                        if let Some(value) = map.remove(p.name()) {
                            v.insert(p.name().to_string(), value.cast(&p.type_, &p.path)?);
                        }
                    }
                    Ok(RecordValue::Map(v))
                }
                _ => error_invalid_field_value_type(self.into(), type_, path),
            },
            Type::Unknown => error_invalid_field_value_type(self.into(), type_, path),
            _ => error_invalid_field_value_type(self.into(), type_, path),
        };
        Ok(v?)
    }

    pub fn validate_prop(&self, prop: &Property) -> Result<()> {
        let type_ = &prop.type_;

        // Check type is valid
        if !self.is_type(type_) {
            return Err(RecordUserError::InvalidFieldValueType {
                value: serde_json::to_value(self)?,
                expected_type: type_.to_string(),
                field: Some(prop.path.path().to_string()),
            })?;
        }

        // Check required type properties
        self.validate_type(&prop.type_)?;

        // Recursively check array/map/object types
        match (&prop.type_, self) {
            (Type::Array(a), RecordValue::Array(v)) => {
                for value in v {
                    value.validate_type(&a.value)?;
                }
                Ok(())
            }
            (Type::Map(m), RecordValue::Map(map)) => {
                for (_, value) in map.iter() {
                    value.validate_type(&m.value)?;
                }
                Ok(())
            }
            (Type::Object(m), RecordValue::Map(map)) => {
                let mut keys = map
                    .keys()
                    .map(|k| (k.as_str(), true))
                    .collect::<HashMap<_, _>>();

                for prop in m.fields.iter() {
                    if let Some(val) = map.get(prop.name()) {
                        keys.remove(prop.name());

                        val.validate_prop(prop)?;
                    } else if prop.required {
                        return Err(RecordUserError::MissingField {
                            field: prop.path.path().to_string(),
                        })?;
                    }
                }

                // Check for extra fields
                if !keys.is_empty() {
                    return Err(RecordUserError::UnexpectedFields {
                        fields: keys
                            .keys()
                            .map(|k| format!("{}.{}", prop.path.path(), k))
                            .collect(),
                    })?;
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn validate_type(&self, type_: &Type) -> Result<()> {
        // Check for required fields
        match (type_, self) {
            (
                Type::ForeignRecord(ForeignRecord { collection }),
                RecordValue::ForeignRecordReference(ForeignRecordReference {
                    collection_id, ..
                }),
            ) => {
                let short_collection_name = collection_id.split('/').last().unwrap_or("");
                if short_collection_name != collection {
                    return Err(
                        RecordUserError::ForeignRecordReferenceHasWrongCollectionId {
                            expected: collection.to_string(),
                            got: short_collection_name.to_string(),
                        },
                    )?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Recursively check if the value is of the given type, for arrays/maps/objects it
    /// checks that all sub-values are of the required type
    pub fn is_type(&self, type_: &Type) -> bool {
        match (type_, self) {
            (Type::Primitive(PrimitiveType::String), RecordValue::String(_)) => true,
            (Type::Primitive(PrimitiveType::Number), RecordValue::Number(_)) => true,
            (Type::Primitive(PrimitiveType::Boolean), RecordValue::Boolean(_)) => true,
            (Type::Primitive(PrimitiveType::Bytes), RecordValue::Bytes(_)) => true,
            (Type::PublicKey, RecordValue::PublicKey(_)) => true,
            (Type::Array(a), RecordValue::Array(v)) => {
                for value in v {
                    if !value.is_type(&a.value) {
                        return false;
                    }
                }
                true
            }
            (Type::Map(m), RecordValue::Map(map)) => {
                for (_, value) in map.iter() {
                    if !value.is_type(&m.value) {
                        return false;
                    }
                }
                true
            }
            (Type::Object(m), RecordValue::Map(map)) => {
                for p in m.fields.iter() {
                    if let Some(value) = map.get(p.name()) {
                        if !value.is_type(&p.type_) {
                            return false;
                        }
                    }
                }
                true
            }
            (Type::Record, RecordValue::RecordReference(_)) => true,
            (Type::ForeignRecord(_), RecordValue::ForeignRecordReference(_)) => true,
            (Type::Unknown, _) => false,
            _ => false,
        }
    }

    // TODO: should we not throw an error here, but filter bad results, as then we could
    // pick up the error more easily during validation?
    pub fn try_from_json_prop(
        prop: &Property,
        value: serde_json::Value,
        force: bool,
    ) -> Result<Self> {
        let type_ = &prop.type_;

        // Try to convert type
        let mut v = Self::try_from_json_type(type_, &prop.path, value, force);

        // Manually assign default value if conversion failed and force is true
        if v.is_err() && force {
            v = Ok(Self::default_from_type(type_));
        }

        // Validate the conversion
        let v = v?;
        v.validate_prop(prop)?;

        Ok(v)
    }

    pub fn try_from_json_type(
        type_: &Type,
        path: &FieldPath,
        value: serde_json::Value,
        force: bool,
    ) -> Result<Self> {
        let v: std::result::Result<RecordValue, RecordUserError> = match type_ {
            // String
            Type::Primitive(PrimitiveType::String) => match value {
                serde_json::Value::String(s) => Ok(RecordValue::String(s)),
                serde_json::Value::Number(n) => Ok(RecordValue::String(n.to_string())),
                serde_json::Value::Bool(b) => Ok(RecordValue::String(b.to_string())),
                serde_json::Value::Array(a) => Ok(RecordValue::String(serde_json::to_string(&a)?)),
                serde_json::Value::Object(b) => Ok(RecordValue::String(serde_json::to_string(&b)?)),
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Number
            Type::Primitive(PrimitiveType::Number) => match value {
                serde_json::Value::Number(n) => Ok(RecordValue::Number(
                    n.as_f64().ok_or(RecordError::FailedToConvertNumberToF64)?,
                )),
                serde_json::Value::String(s) => Ok(RecordValue::Number(s.parse::<f64>()?)),
                serde_json::Value::Bool(b) => Ok(RecordValue::Number(if b { 1.0 } else { 0.0 })),
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Boolean
            Type::Primitive(PrimitiveType::Boolean) => match value {
                serde_json::Value::Bool(b) => Ok(RecordValue::Boolean(b)),
                serde_json::Value::Null => Ok(RecordValue::Boolean(false)),
                serde_json::Value::Number(n) => {
                    Ok(RecordValue::Boolean(n.as_f64().unwrap_or(0.0) != 0.0))
                }
                serde_json::Value::String(s) => Ok(RecordValue::Boolean(s == "true")),
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Bytes
            Type::Primitive(PrimitiveType::Bytes) => match value {
                serde_json::Value::String(s) => Ok(RecordValue::Bytes(
                    base64::engine::general_purpose::STANDARD.decode(s.as_bytes())?,
                )),
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Public Key
            Type::PublicKey => match value {
                serde_json::Value::Object(o) => {
                    Ok(RecordValue::PublicKey(publickey::PublicKey::try_from(o)?))
                }
                serde_json::Value::String(s) => {
                    Ok(RecordValue::PublicKey(publickey::PublicKey::from_hex(&s)?))
                }
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Array
            Type::Array(a) => match value {
                serde_json::Value::Array(array) => {
                    let mut v = Vec::with_capacity(array.len());
                    for value in array {
                        v.push(RecordValue::try_from_json_type(
                            &a.value, path, value, force,
                        )?);
                    }
                    Ok(RecordValue::Array(v))
                }
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Map
            Type::Map(m) => match value {
                serde_json::Value::Object(map) => {
                    let mut v = HashMap::with_capacity(map.len());
                    for (key, value) in map {
                        v.insert(
                            key.clone(),
                            RecordValue::try_from_json_type(&m.value, path, value, force)?,
                        );
                    }
                    Ok(RecordValue::Map(v))
                }
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Object
            Type::Object(m) => match value {
                serde_json::Value::Object(mut map) => {
                    let mut v = HashMap::with_capacity(map.len());
                    for p in m.fields.iter() {
                        if let Some(value) = map.remove(p.name()) {
                            v.insert(
                                p.name().to_string(),
                                RecordValue::try_from_json_prop(p, value, force)?,
                            );
                        }
                    }
                    Ok(RecordValue::Map(v))
                }
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // // Record
            Type::Record => match value {
                serde_json::Value::Object(o) => {
                    Ok(RecordValue::RecordReference(RecordReference::try_from(o)?))
                }
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Foreign Record
            Type::ForeignRecord(_) => match value {
                serde_json::Value::Object(o) => Ok(RecordValue::ForeignRecordReference(
                    ForeignRecordReference::try_from(o)?,
                )),
                _ => error_invalid_field_value_type(value, type_, path),
            },
            // Anything else is an error
            _ => error_invalid_field_value_type(value, type_, path),
        };

        Ok(v?)
    }

    pub fn default_from_type(type_: &Type) -> Self {
        match type_ {
            Type::Primitive(p) => match p {
                PrimitiveType::String => RecordValue::String("".to_string()),
                PrimitiveType::Number => RecordValue::Number(0f64),
                PrimitiveType::Boolean => RecordValue::Boolean(false),
                PrimitiveType::Bytes => RecordValue::Bytes(vec![]),
            },
            Type::Array(_) => RecordValue::Array(vec![]),
            Type::Map(_) => RecordValue::Map(HashMap::new()),
            Type::Record => RecordValue::RecordReference(RecordReference::default()),
            Type::ForeignRecord(_) => {
                RecordValue::ForeignRecordReference(ForeignRecordReference::default())
            }
            Type::Object(_) => RecordValue::Map(HashMap::new()),
            Type::PublicKey => RecordValue::PublicKey(publickey::PublicKey::default()),
            // TODO: should we return a Result Err here instead?
            Type::Unknown => RecordValue::String("UNKNOWN_VALUE".to_string()),
        }
    }
}

fn error_invalid_field_value_type(
    value: serde_json::Value,
    expected_type: &Type,
    path: &FieldPath,
) -> std::result::Result<RecordValue, RecordUserError> {
    Err(RecordUserError::InvalidFieldValueType {
        value,
        expected_type: expected_type.to_string(),
        field: Some(path.to_string()),
    })?
}

impl From<RecordValue> for serde_json::Value {
    fn from(value: RecordValue) -> Self {
        match value {
            RecordValue::String(s) => serde_json::Value::String(s),
            // TODO: what to do with NaN or infinite values? Would those even occur?
            RecordValue::Number(n) => serde_json::Number::from_f64(n)
                .unwrap_or(serde_json::Number::from(0))
                .into(),
            RecordValue::Boolean(b) => serde_json::Value::Bool(b),
            RecordValue::PublicKey(p) => serde_json::Value::from(p),
            RecordValue::Null => serde_json::Value::Null,
            RecordValue::Bytes(b) => {
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
            RecordValue::Map(m) => {
                let mut map = serde_json::Map::with_capacity(m.len());
                for (k, v) in m {
                    map.insert(k, serde_json::Value::from(v));
                }
                serde_json::Value::Object(map)
            }
            RecordValue::Array(a) => {
                let mut array = Vec::with_capacity(a.len());
                for v in a {
                    array.push(serde_json::Value::from(v));
                }
                serde_json::Value::Array(array)
            }
            RecordValue::RecordReference(r) => serde_json::Value::from(r),
            RecordValue::ForeignRecordReference(r) => serde_json::Value::from(r),
        }
    }
}

impl RecordValue {
    // TODO: Remove
    pub fn walk<'a, E: std::error::Error>(
        &'a self,
        current_path: &mut Vec<Cow<'a, str>>,
        f: &mut impl FnMut(&[Cow<str>], IndexValue<'a>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            RecordValue::Number(n) => {
                f(current_path, IndexValue::Number(*n))?;
            }
            RecordValue::Boolean(b) => {
                f(current_path, IndexValue::Boolean(*b))?;
            }
            RecordValue::Null => {
                f(current_path, IndexValue::Null)?;
            }
            RecordValue::String(s) => {
                f(current_path, IndexValue::String(Cow::Borrowed(s)))?;
            }
            RecordValue::PublicKey(p) => {
                f(current_path, IndexValue::PublicKey(Cow::Borrowed(p)))?;
            }
            RecordValue::Bytes(_) => {}
            RecordValue::Map(m) => {
                for (k, v) in m.iter() {
                    current_path.push(Cow::Borrowed(k));
                    v.walk(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    current_path.push(Cow::Owned(i.to_string()));
                    v.walk(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::RecordReference(_) => {}
            RecordValue::ForeignRecordReference(fr) => {
                f(
                    current_path,
                    IndexValue::ForeignRecordReference(Cow::Borrowed(fr)),
                )?;
            }
        }

        Ok(())
    }

    pub fn walk_all<'a, E: std::error::Error>(
        &'a self,
        current_path: &mut Vec<Cow<'a, str>>,
        f: &mut impl FnMut(&[Cow<str>], &'a RecordValue) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            RecordValue::String(_) => {
                f(current_path, self)?;
            }
            RecordValue::Number(_) => {
                f(current_path, self)?;
            }
            RecordValue::Boolean(_) => {
                f(current_path, self)?;
            }
            RecordValue::PublicKey(_) => {
                f(current_path, self)?;
            }
            RecordValue::Null => {
                f(current_path, self)?;
            }
            RecordValue::Bytes(_) => {
                f(current_path, self)?;
            }
            RecordValue::Map(m) => {
                f(current_path, self)?;

                for (k, v) in m.iter() {
                    current_path.push(Cow::Borrowed(k));
                    v.walk_all(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::Array(a) => {
                f(current_path, self)?;

                for (i, v) in a.iter().enumerate() {
                    current_path.push(Cow::Owned(i.to_string()));
                    v.walk_all(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::RecordReference(_) => {
                f(current_path, self)?;
            }
            RecordValue::ForeignRecordReference(_) => {
                f(current_path, self)?;
            }
        }

        Ok(())
    }

    pub fn walk_maps_mut<'rv, E>(
        &mut self,
        current_path: &mut Vec<Cow<'rv, str>>,
        f: &mut impl FnMut(
            &[Cow<'rv, str>],
            &mut HashMap<String, RecordValue>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            RecordValue::String(_) => {}
            RecordValue::Number(_) => {}
            RecordValue::Boolean(_) => {}
            RecordValue::PublicKey(_) => {}
            RecordValue::Null => {}
            RecordValue::Bytes(_) => {}
            RecordValue::Map(m) => {
                f(current_path, m)?;
                let keys = m.keys().cloned().collect::<Vec<_>>();
                for (k, v) in keys.into_iter().zip(m.values_mut()) {
                    current_path.push(Cow::Owned(k));
                    v.walk_maps_mut(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::Array(a) => {
                for (i, v) in a.iter_mut().enumerate() {
                    current_path.push(Cow::Owned(i.to_string()));
                    v.walk_maps_mut(current_path, f)?;
                    current_path.pop();
                }
            }
            RecordValue::RecordReference(_) => {}
            RecordValue::ForeignRecordReference(_) => {}
        }

        Ok(())
    }
}

/// A reference to another record (either same collection or foreign)
#[derive(Debug)]
pub enum Reference<'a> {
    Record(&'a RecordReference),
    ForeignRecord(&'a ForeignRecordReference),
}

/// A reference to a record in the same collection
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct RecordReference {
    pub id: String,
}

impl TryFrom<serde_json::Map<String, serde_json::Value>> for RecordReference {
    type Error = RecordUserError;

    fn try_from(
        mut o: serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<Self, RecordUserError> {
        let id = get_reference_field_value(&mut o, "id")?;
        Ok(Self { id })
    }
}

impl From<RecordReference> for serde_json::Value {
    fn from(r: RecordReference) -> Self {
        serde_json::json!({
            "id": r.id,
        })
    }
}

/// A reference to a record in a different collection
#[derive(Debug, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Default)]
pub struct ForeignRecordReference {
    pub id: String,
    #[serde(rename = "collectionId")]
    pub collection_id: String,
}

impl ForeignRecordReference {
    pub fn to_indexable(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&u32::to_be_bytes(self.collection_id.as_bytes().len() as u32));
        v.extend_from_slice(self.collection_id.as_bytes());
        v.extend_from_slice(&u32::to_be_bytes(self.id.as_bytes().len() as u32));
        v.extend_from_slice(self.id.as_bytes());
        v
    }

    pub fn from_indexable(v: &[u8]) -> Result<Self> {
        let mut v = v;
        let collection_id_len = u32::from_be_bytes(v[..4].try_into()?) as usize;
        v = &v[4..];
        let collection_id = String::from_utf8(v[..collection_id_len].to_vec())?;
        v = &v[collection_id_len..];
        let id_len = u32::from_be_bytes(v[..4].try_into()?) as usize;
        v = &v[4..];
        let id = String::from_utf8(v[..id_len].to_vec())?;
        Ok(ForeignRecordReference { id, collection_id })
    }
}

impl From<ForeignRecordReference> for serde_json::Value {
    fn from(r: ForeignRecordReference) -> Self {
        serde_json::json!({
            "id": r.id,
            "collectionId": r.collection_id,
        })
    }
}

impl TryFrom<serde_json::Map<String, serde_json::Value>> for ForeignRecordReference {
    type Error = RecordUserError;

    fn try_from(
        mut o: serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<Self, RecordUserError> {
        let id = get_reference_field_value(&mut o, "id")?;
        let collection_id = get_reference_field_value(&mut o, "collectionId")
            .or_else(|_| get_reference_field_value(&mut o, "__collectionId"))?;
        Ok(Self { id, collection_id })
    }
}

fn get_reference_field_value(
    map: &mut serde_json::Map<String, serde_json::Value>,
    prop: &str,
) -> std::result::Result<String, RecordUserError> {
    let val = match map.remove(prop) {
        Some(serde_json::Value::String(s)) => s,
        Some(_) => {
            return Err(RecordUserError::RecordReferenceInvalidType {
                field: prop.to_string(),
            })?
        }
        None => {
            return Err(RecordUserError::RecordReferenceMissingField {
                field: prop.to_string(),
            })?
        }
    };
    Ok(val)
}

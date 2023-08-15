use crate::{
    field_path::FieldPath,
    property::PropertyList,
    publickey::PublicKey,
    record::{ForeignRecordReference, RecordReference, RecordValue},
};
use base64::Engine;
use polylang::stableast;
use std::{boxed::Box, collections::HashMap, fmt::Display};

pub use stableast::PrimitiveType;

#[derive(Debug, PartialEq, Clone)]
pub enum Type {
    Primitive(PrimitiveType),
    PublicKey,
    Array(Array),
    Map(Map),
    Object(Object),
    Record,
    ForeignRecord(ForeignRecord),
    Unknown,
}

impl Type {
    pub fn from_ast(type_: &stableast::Type<'_>, path: &FieldPath) -> Self {
        match type_ {
            stableast::Type::Primitive(t) => Self::Primitive(t.value.clone()),
            stableast::Type::Array(t) => Self::Array(Array {
                value: Box::new(Self::from_ast(&t.value, path)),
            }),
            stableast::Type::Map(t) => Self::Map(Map {
                key: Box::new(Self::from_ast(&t.key, path)),
                value: Box::new(Self::from_ast(&t.value, path)),
            }),
            stableast::Type::Record(_) => Self::Record,
            stableast::Type::ForeignRecord(t) => Self::ForeignRecord(ForeignRecord {
                collection: t.collection.to_string(),
            }),
            stableast::Type::PublicKey(_) => Self::PublicKey,
            stableast::Type::Object(o) => Self::Object(Object {
                fields: PropertyList::from_ast_object(o, path),
            }),
            stableast::Type::Unknown => Self::Unknown,
        }
    }

    pub fn is_indexable(&self) -> bool {
        matches!(
            self,
            Type::Primitive(PrimitiveType::Boolean)
                | Type::Primitive(PrimitiveType::String)
                | Type::Primitive(PrimitiveType::Number)
                | Type::Record
                | Type::ForeignRecord(_)
                | Type::PublicKey
        )
    }

    pub fn is_authenticable(&self) -> bool {
        matches!(
            self,
            Type::PublicKey | Type::Record | Type::ForeignRecord(_)
        )
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Primitive(p) => write!(f, "{}", p),
            Type::Array(a) => write!(f, "{}[]", a.value),
            Type::Map(m) => write!(f, "map<{}, {}>", m.key, m.value),
            Type::Object(o) => {
                write!(f, "{{ ")?;
                for (_, field) in o.fields.iter().enumerate() {
                    write!(
                        f,
                        "{}{}: {}",
                        field.path,
                        if field.required { "" } else { "?" },
                        field.type_
                    )?;
                    write!(f, ";")?;
                    write!(f, " ")?;
                }
                write!(f, "}}")
            }
            Type::Record => write!(f, "record"),
            Type::ForeignRecord(fr) => write!(f, "{}", fr.collection),
            Type::PublicKey => write!(f, "PublicKey"),
            Type::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Array {
    pub value: Box<Type>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Map {
    pub key: Box<Type>,
    pub value: Box<Type>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignRecord {
    pub collection: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Object {
    pub fields: PropertyList,
}

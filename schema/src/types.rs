use polylang::stableast;
use std::boxed::Box;

pub use stableast::PrimitiveType;

use crate::{field_path::FieldPath, property::PropertyList};

#[derive(Debug, PartialEq, Clone)]
pub enum Type {
    Primitive(PrimitiveType),
    Array(Array),
    Map(Map),
    Record,
    Object(Object),
    ForeignRecord(ForeignRecord),
    PublicKey,
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
            Type::Primitive(_) | Type::Record | Type::ForeignRecord(_) | Type::PublicKey
        )
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

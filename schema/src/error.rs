use crate::methods;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    User(#[from] UserError),

    #[error("methods error: {0}")]
    Method(#[from] methods::UserError),

    #[error("serde_json error")]
    SerdeJSON(#[from] serde_json::Error),

    #[error("collection record missing AST")]
    CollectionRecordMissingAST,

    #[error("collection record missing ID")]
    CollectionRecordMissingID,

    #[error("collection {name} not found in AST")]
    CollectionNotFoundInAST { name: String },

    #[error("collection record ID is not a string")]
    CollectionRecordIDIsNotAString,

    #[error("collection record AST is not a string")]
    CollectionRecordASTIsNotAString,
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("collection id is missing namespace")]
    CollectionIdMissingNamespace,

    #[error("collection name cannot start with '$'")]
    CollectionNameCannotStartWithDollarSign,

    #[error("collection must have an 'id' field")]
    CollectionMissingIdField,

    #[error("collection 'id' field must be a string")]
    CollectionIdFieldMustBeString,

    #[error("collection 'id' field cannot be optional")]
    CollectionIdFieldCannotBeOptional,

    #[error("index field {field:?} not found in schema")]
    IndexFieldNotFoundInSchema { field: String },

    #[error("cannot index field \"{field}\" of type {field_type}")]
    FieldTypeCannotBeIndexed { field: String, field_type: String },

    #[error("cannot change type of fields: \"{fields}\", delete the fields and re-create them")]
    SchemaFieldTypeChangeNotAllowed { fields: String },

    #[error("collection directive {directive:?} cannot have arguments")]
    CollectionDirectiveCannotHaveArguments { directive: String },

    #[error("unknown collection directives {directives:?}")]
    UnknownCollectionDirectives { directives: Vec<String> },
}

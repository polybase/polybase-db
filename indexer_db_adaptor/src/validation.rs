use super::{
    ast::collection_ast_from_root,
    error::{CollectionError, CollectionUserError, Result},
};
use polylang::stableast;
use schema::{
    record::{RecordRoot, RecordValue},
    util,
};
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("collection id is missing namespace")]
    CollectionIdMissingNamespace,
}

// TODO: we need to do more checks than this - e.g. we need to check if a field is changing type
// which we should not allow

#[tracing::instrument]
pub fn validate_schema_change(
    collection_name: &str,
    old_ast: stableast::Root,
    new_ast: stableast::Root,
) -> Result<()> {
    let Some(_old_ast) = collection_ast_from_root(old_ast, collection_name) else {
        return Err(CollectionError::CollectionNotFoundInAST { name: collection_name.to_string() });
    };
    let Some(_new_ast) = collection_ast_from_root(new_ast, collection_name) else {
        return Err(CollectionError::CollectionNotFoundInAST { name: collection_name.to_string() });
    };

    Ok(())
}

// TODO: we need to check this before allowing a change to a schema record, technically this should
// be in the

#[tracing::instrument(skip(record))]
pub fn validate_collection_record(record: &RecordRoot) -> Result<()> {
    let (namespace, name) = if let Some(RecordValue::String(id)) = record.get("id") {
        let Some((namespace, name)) = id.rsplit_once('/') else {
                return Err(CollectionUserError::CollectionIdMissingNamespace)?;
            };

        (namespace, name)
    } else {
        unreachable!()
    };

    if namespace.is_empty() {
        return Err(CollectionUserError::CollectionIdMissingNamespace.into());
    }

    if name.starts_with('$') {
        return Err(CollectionUserError::CollectionNameCannotStartWithDollarSign.into());
    }

    let Some(ast) = record.get("ast") else {
        return Err(CollectionError::CollectionRecordMissingAST);
    };

    let ast = match ast {
        RecordValue::String(ast) => ast,
        _ => return Err(CollectionError::CollectionRecordASTIsNotAString),
    };

    let ast = serde_json::from_str::<polylang::stableast::Root>(ast)?;

    let Some(collection) = collection_ast_from_root(ast, &util::normalize_name(name)) else {
        return Err(CollectionUserError::MissingDefinitionForCollection { name: name.to_owned() }.into());
    };

    // Create the schema and use it to validate
    let schema = schema::Schema::new(&collection);
    schema.validate()?;

    Ok(())
}

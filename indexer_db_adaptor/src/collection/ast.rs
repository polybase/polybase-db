use super::{
    error::{CollectionError, Result},
    record::{RecordRoot, RecordValue},
};
use polylang::stableast;

pub fn collection_ast_from_root<'a>(
    ast: stableast::Root<'a>,
    collection_name: &str,
) -> Option<stableast::Collection<'a>> {
    ast.0.into_iter().find_map(|node| match node {
        polylang::stableast::RootNode::Collection(collection)
            if collection.name == collection_name =>
        {
            Some(collection)
        }
        _ => None,
    })
}

pub fn collection_ast_from_record<'a>(
    record: &'a RecordRoot,
    collection_name: &str,
) -> Result<stableast::Collection<'a>> {
    let collection_ast: stableast::Collection = match record.get("ast") {
        Some(RecordValue::String(ast)) => collection_ast_from_json(ast, collection_name)?,
        Some(_) => return Err(CollectionError::CollectionRecordASTIsNotAString),
        None => return Err(CollectionError::CollectionRecordMissingAST),
    };
    Ok(collection_ast)
}

#[tracing::instrument]
pub fn collection_ast_from_json<'a>(
    ast_json: &'a str,
    collection_name: &str,
) -> Result<stableast::Collection<'a>> {
    let ast = serde_json::from_str::<polylang::stableast::Root>(ast_json)?;
    let Some(collection_ast) = collection_ast_from_root(ast, collection_name) else {
        return Err(CollectionError::CollectionNotFoundInAST { name: collection_name.to_string() });
    };

    Ok(collection_ast)
}

pub fn fields_from_ast<'a>(
    collection_ast: &'a stableast::Collection<'a>,
) -> impl Iterator<Item = &stableast::Property<'a>> {
    collection_ast.attributes.iter().filter_map(|a| match a {
        polylang::stableast::CollectionAttribute::Property(p) => Some(p),
        _ => None,
    })
}

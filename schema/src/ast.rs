use super::{
    error::{Error, Result},
    record::{RecordRoot, RecordValue},
};
use polylang::stableast;

pub fn collection_ast_from_record<'a>(
    collection_name: &str,
    record: &'a RecordRoot,
) -> Result<stableast::Collection<'a>> {
    let collection_ast: stableast::Collection = match record.get("ast") {
        Some(RecordValue::String(ast)) => collection_ast_from_json_str(collection_name, ast)?,
        Some(_) => return Err(Error::CollectionRecordASTIsNotAString),
        None => return Err(Error::CollectionRecordMissingAST),
    };
    Ok(collection_ast)
}

pub fn collection_ast_from_json_str<'a>(
    collection_name: &str,
    ast_json: &'a str,
) -> Result<stableast::Collection<'a>> {
    let ast = serde_json::from_str::<polylang::stableast::Root>(ast_json)?;
    let Some(collection_ast) = collection_ast_from_root(collection_name, ast) else {
        return Err(Error::CollectionNotFoundInAST { name: collection_name.to_string() });
    };

    Ok(collection_ast)
}

// pub fn collection_ast_from_json_value(
//     ast_json: serde_json::Value,
//     collection_name: &str,
// ) -> Result<stableast::Collection<'a>> {
//     let ast = serde_json::from_value::<polylang::stableast::Root>(ast_json)?;
//     let Some(collection_ast) = collection_ast_from_root(ast, collection_name) else {
//         return Err(Error::CollectionNotFoundInAST { name: collection_name.to_string() });
//     };

//     Ok(collection_ast)
// }

pub fn collection_ast_from_root<'a>(
    collection_name: &str,
    ast: stableast::Root<'a>,
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

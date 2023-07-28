use super::{
    error::{CollectionError, Result},
    index::{Index, IndexDirection, IndexField},
    record::{RecordRoot, RecordValue},
    stableast_ext::FieldWalker,
};
use polylang::stableast;
use std::borrow::Cow;

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

pub fn indexes_from_ast(collection_ast: &stableast::Collection<'_>) -> Vec<Index<'static>> {
    // Extract manually defined indexes
    let mut indexes = collection_ast
        .attributes
        .iter()
        .filter_map(|attr| match attr {
            stableast::CollectionAttribute::Index(index) => Some(Index::new(
                index
                    .fields
                    .iter()
                    .map(|field| {
                        IndexField::new(
                            field
                                .field_path
                                .iter()
                                .map(|p| Cow::Owned(p.to_string()))
                                .collect(),
                            match field.direction {
                                stableast::Direction::Asc => IndexDirection::Ascending,
                                stableast::Direction::Desc => IndexDirection::Descending,
                            },
                        )
                    })
                    .collect(),
            )),
            _ => None,
        })
        .chain([Index::new(vec![])].into_iter())
        .collect::<Vec<_>>();

    // Add all automatically indexed fields in a collection to the list of Indexes
    collection_ast.walk_fields(&mut vec![], &mut |path, field| {
        let indexable = matches!(
            field.type_(),
            stableast::Type::Primitive(_) | stableast::Type::PublicKey(_)
        );

        if indexable {
            let new_index = |direction| {
                Index::new(vec![IndexField::new(
                    path.iter().map(|p| Cow::Owned(p.to_string())).collect(),
                    direction,
                )])
            };
            let new_index_asc = new_index(IndexDirection::Ascending);
            let new_index_desc = new_index(IndexDirection::Descending);

            if !indexes.contains(&new_index_asc) && !indexes.contains(&new_index_desc) {
                indexes.push(new_index_asc);
            }
        }
    });

    // Sort indexes by number of fields, so that we use the most specific index first
    indexes.sort_by(|a, b| a.fields.len().cmp(&b.fields.len()));

    indexes
}

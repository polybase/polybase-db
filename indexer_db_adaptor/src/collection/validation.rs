use super::{
    ast::collection_ast_from_root,
    error::{CollectionError, CollectionUserError, Result},
    record::{RecordRoot, RecordValue},
    stableast_ext::FieldWalker,
    util,
};
use polylang::stableast;
use tracing::warn;

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

    let properties = collection
        .attributes
        .iter()
        .filter_map(|a| match a {
            stableast::CollectionAttribute::Property(p) => Some(p),
            _ => None,
        })
        .collect::<Vec<_>>();

    let Some(id_property) = properties.iter().find(|p| p.name == "id") else {
        return Err(CollectionUserError::CollectionMissingIdField.into());
    };

    if id_property.type_
        != stableast::Type::Primitive(stableast::Primitive {
            value: stableast::PrimitiveType::String,
        })
    {
        return Err(CollectionUserError::CollectionIdFieldMustBeString.into());
    }

    if !id_property.required {
        return Err(CollectionUserError::CollectionIdFieldCannotBeOptional.into());
    }

    let indexes = collection
        .attributes
        .iter()
        .filter_map(|a| match a {
            stableast::CollectionAttribute::Index(i) => Some(i),
            _ => None,
        })
        .collect::<Vec<_>>();

    for index in indexes {
        for index_field in &index.fields {
            let Some(field) = collection.find_field(&index_field.field_path) else {
                return Err(CollectionUserError::IndexFieldNotFoundInSchema {
                    field: index_field.field_path.join("."),
                }
                .into());
            };

            match field.type_() {
                stableast::Type::Array(_) => {
                    return Err(CollectionUserError::IndexFieldCannotBeAnArray {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                stableast::Type::Map(_) => {
                    return Err(CollectionUserError::IndexFieldCannotBeAMap {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                stableast::Type::Object(_) => {
                    return Err(CollectionUserError::IndexFieldCannotBeAnObject {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                stableast::Type::Primitive(stableast::Primitive {
                    value: stableast::PrimitiveType::Bytes,
                }) => {
                    return Err(CollectionUserError::IndexFieldCannotBeBytes {
                        field: index_field.field_path.join("."),
                    }
                    .into());
                }
                _ => {}
            }
        }
    }

    let directives = collection
        .attributes
        .iter()
        .filter_map(|a| match a {
            stableast::CollectionAttribute::Directive(d) => Some(d),
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(public_directive) = directives.iter().find(|d| d.name == "public") {
        if !public_directive.arguments.is_empty() {
            return Err(
                CollectionUserError::CollectionDirectiveCannotHaveArguments {
                    directive: "public",
                }
                .into(),
            );
        }
    }
    if let Some(read_directive) = directives.iter().find(|d| d.name == "read") {
        if !read_directive.arguments.is_empty() {
            return Err(
                CollectionUserError::CollectionDirectiveCannotHaveArguments { directive: "read" }
                    .into(),
            );
        }
    }
    if let Some(call_directive) = directives.iter().find(|d| d.name == "call") {
        if !call_directive.arguments.is_empty() {
            return Err(
                CollectionUserError::CollectionDirectiveCannotHaveArguments { directive: "call" }
                    .into(),
            );
        }
    }

    const VALID_COLLECTION_DIRECTIVES: &[&str] = &["public", "read", "call"];
    let unknown_directives = directives
        .iter()
        .filter(|d| !VALID_COLLECTION_DIRECTIVES.contains(&d.name.as_ref()))
        .map(|d| d.name.as_ref().to_owned())
        .collect::<Vec<_>>();
    if !unknown_directives.is_empty() {
        return Err(CollectionUserError::UnknownCollectionDirectives {
            directives: unknown_directives,
        }
        .into());
    }

    Ok(())
}

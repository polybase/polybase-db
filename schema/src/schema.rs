use crate::{
    ast::{collection_ast_from_json_str, collection_ast_from_record},
    directive::{Directive, DirectiveKind},
    error::{Error, Result, UserError},
    field_path::FieldPath,
    index::{custom_indexes_from_ast, Index, IndexField},
    methods::Method,
    property::{Property, PropertyList},
    publickey::{self, PublicKey},
    record::{RecordRoot, RecordValue, Reference},
    types::{PrimitiveType, Type},
};
use polylang::stableast;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

// TODO: can we remove Clone
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Schema {
    pub name: String,
    /// List of indexes in the Collection, includes both automatic field indexes and custom
    /// indexes defined by the user. Indexes are sorted by more specific to less specific.
    pub indexes: Vec<Index>,
    // TODO: should we include all directives here? Or at least create an iterator for them?
    /// List of collection Directives (e.g. @public, @call, @read)
    pub root_directives: Vec<Directive>,
    /// List of properties in the Collection
    pub properties: PropertyList,
    /// List of methods in the Collection
    pub methods: HashMap<String, Method>,
    /// Anyone can read the collection
    pub read_all: bool,
    /// Anyone can call the collection functions
    pub call_all: bool,
}

impl Schema {
    pub fn new(collection_ast: &stableast::Collection) -> Self {
        let properties = PropertyList::from_ast_collection(collection_ast);

        // Get a vec of all indexes
        let mut indexes = custom_indexes_from_ast(collection_ast);
        properties
            .iter()
            .filter(|p| p.type_.is_indexable())
            .for_each(|p| {
                let new_index_asc = Index::new(vec![IndexField::new_asc(p.path.clone())]);
                let new_index_desc = Index::new(vec![IndexField::new_desc(p.path.clone())]);
                if !indexes.contains(&new_index_asc) && !indexes.contains(&new_index_desc) {
                    indexes.push(new_index_asc);
                }
            });

        // Sort indexes by number of fields, so that we use the most specific index first
        indexes.sort_by(|a, b| a.fields.len().cmp(&b.fields.len()));

        // Get a list of root directives (e.g. @public, @call, @read that apply to the whole collection)
        let root_directives = collection_ast
            .attributes
            .iter()
            .filter_map(|a| match a {
                stableast::CollectionAttribute::Directive(d) => {
                    Some(Directive::from_ast_directive(d))
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        // Get a list of methods
        let mut methods = HashMap::new();
        collection_ast.attributes.iter().for_each(|a| {
            if let stableast::CollectionAttribute::Method(d) = a {
                let method = Method::from_ast_method(d);
                methods.insert(method.name.clone(), method);
            }
        });

        let is_public = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "public"));
        let read_all = is_public || collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "read" && d.arguments.is_empty()));
        let call_all = is_public || collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "call" && d.arguments.is_empty()));

        Self {
            name: collection_ast.name.to_string(),
            root_directives,
            methods,
            indexes,
            read_all,
            call_all,
            properties: PropertyList::from_ast_collection(collection_ast),
        }
    }

    pub fn from_record(record: &RecordRoot) -> Result<Self> {
        let id = match record.get("id") {
            Some(RecordValue::String(id)) => id,
            None => return Err(Error::CollectionRecordMissingID),
            _ => return Err(Error::CollectionRecordIDIsNotAString),
        };

        let (namespace, name) = if let Some((namespace, name)) = id.rsplit_once('/') {
            (namespace, name)
        } else {
            return Err(UserError::CollectionIdMissingNamespace)?;
        };

        if namespace.is_empty() {
            return Err(UserError::CollectionIdMissingNamespace.into());
        }

        // TODO: should we move this to Polybase?
        if name.starts_with('$') {
            return Err(UserError::CollectionNameCannotStartWithDollarSign.into());
        }

        let collection = collection_ast_from_record(name, record)?;

        // Create the schema and use it to validate
        let schema = Self::new(&collection);

        schema.validate()?;

        Ok(schema)
    }

    pub fn from_json_str(name: &str, ast_as_json_str: &str) -> Result<Self> {
        Ok(Self::new(&collection_ast_from_json_str(
            name,
            ast_as_json_str,
        )?))
    }

    /// Checks if a public key property with required directive exists, and public
    /// key matches the provided public key
    pub fn authorise_directives_with_public_key(
        &self,
        directives: &[DirectiveKind],
        record: &RecordRoot,
        public_key: &publickey::PublicKey,
    ) -> bool {
        // Find any public key properties, matching provided public key
        self.fields_auth(directives)
            .any(match_public_key(record, public_key))
    }

    /// Checks both field properties and method directives for public key properties
    /// that match the provided public key
    pub fn authorise_method_with_public_key(
        &self,
        method: &str,
        record: &RecordRoot,
        public_key: &publickey::PublicKey,
    ) -> bool {
        // Check fields AND methods for public key
        self.fields_auth(&[DirectiveKind::Call])
            .chain(self.method_auth(method))
            .any(match_public_key(record, public_key))
    }

    /// Finds all fields that are references with the provided directives attached
    pub fn find_directive_references<'a>(
        &'a self,
        directives: &'a [DirectiveKind],
        record: &'a RecordRoot,
    ) -> impl Iterator<Item = (FieldPath, Reference<'a>)> {
        // Find any auth references
        unique_filter(
            self.fields_auth(directives)
                .filter_map(map_to_reference(record)),
            |(path, _)| path,
        )
    }

    /// Finds all fields including fields referenced in method directives arguments
    pub fn find_method_references<'a>(
        &'a self,
        method: &str,
        record: &'a RecordRoot,
    ) -> impl Iterator<Item = (FieldPath, Reference<'a>)> {
        // Find any auth references
        unique_filter(
            self.fields_auth(&[DirectiveKind::Call])
                .chain(self.method_auth(method))
                .filter_map(map_to_reference(record)),
            |(path, _)| path,
        )
    }

    /// Get all properties from @call directives on a method, e.g @call(publicKey, anotherField)
    pub fn method_auth(&self, method: &str) -> impl Iterator<Item = &Property> {
        self.methods
            .get(method)
            .map(|m| {
                let mut properties = vec![];
                m.directives
                    .iter()
                    .filter(|d| d.kind == DirectiveKind::Call)
                    .for_each(|d| {
                        d.arguments
                            .iter()
                            .filter_map(|arg| self.properties.get_path(arg))
                            .filter(|p| p.type_.is_authenticable())
                            .for_each(|p| properties.push(p));
                    });
                properties.into_iter()
            })
            .unwrap_or(vec![].into_iter())
    }

    /// Finds all properties/fields with a type that can be authenticated and a directive is used
    pub fn fields_auth<'a>(
        &'a self,
        directives: &'a [DirectiveKind],
    ) -> impl Iterator<Item = &Property> {
        self.properties.iter_all().filter(|p| {
            p.directives
                .iter()
                .any(|directive| directives.contains(&directive.kind))
                && p.type_.is_authenticable()
        })
    }

    pub fn get_method(&self, method: &str) -> Option<&Method> {
        self.methods.get(method)
    }

    pub fn generate_js(&self) -> String {
        let fns = self
            .methods
            .values()
            .map(|method| format!("instance.{} = {}", method.name, method.generate_js()))
            .collect::<Vec<String>>()
            .join(";");

        format!(
            "function error(str) {{
                return new Error(str);
            }}
            
            const instance = $$__instance;
            {};",
            fns,
        )
    }

    // TODO: validate that we can change to the new schem we need to do more checks than this
    // - e.g. we need to check if a field is changing type which we should not allow
    pub fn validate_schema_change(&self, new_schema: Schema) -> Result<()> {
        todo!()
    }

    pub fn validate(&self) -> Result<()> {
        // We don't need to validate the in-built collection as we know this will be valid
        if self.name == "Collection" {
            return Ok(());
        }

        // Ensure we have an ID field
        let Some(id_property) = self.properties.iter().find(|p| p.name() == "id") else {
            return Err(UserError::CollectionMissingIdField.into());
        };

        // Ensure ID field is a string
        // TODO: this will be removed soon, as we will allow other types for ID field
        if id_property.type_ != Type::Primitive(PrimitiveType::String) {
            return Err(UserError::CollectionIdFieldMustBeString.into());
        }

        // Ensure ID field is required
        if !id_property.required {
            return Err(UserError::CollectionIdFieldCannotBeOptional.into());
        }

        // Validate indexes
        for index in self.indexes.iter() {
            for index_field in &index.fields {
                let Some(prop) = self.properties.iter_all().find(|p| p.path == index_field.path) else {
                    return Err(UserError::IndexFieldNotFoundInSchema {
                        field: index_field.path.to_string(),
                    }
                    .into());
                };

                if !prop.type_.is_indexable() {
                    return Err(UserError::FieldTypeCannotBeIndexed {
                        field: index_field.path.to_string(),
                        field_type: prop.type_.to_string(),
                    }
                    .into());
                }
            }
        }

        // Validate collection directives
        let invalid_root_directives: Vec<String> = self
            .root_directives
            .iter()
            .filter(|d| !d.kind.allow_root())
            .map(|d| d.kind.to_string())
            .collect();

        // Validate that we don't have any unknown directives
        if !invalid_root_directives.is_empty() {
            return Err(UserError::UnknownCollectionDirectives {
                directives: invalid_root_directives,
            }
            .into());
        }

        // Validate that we don't have any arguments on collection directives
        if let Some(directive) = self
            .root_directives
            .iter()
            .find(|d| !d.arguments.is_empty())
        {
            return Err(UserError::CollectionDirectiveCannotHaveArguments {
                directive: directive.kind.to_string(),
            }
            .into());
        }

        Ok(())
    }
}

/// Performs a match on a property to determine if it is a public key and matches the provided
/// public key
fn match_public_key<'a>(
    record: &'a RecordRoot,
    public_key: &'a PublicKey,
) -> impl Fn(&'a Property) -> bool {
    move |p| {
        matches!(p.type_, Type::PublicKey)
            && match record.get_path(&p.path) {
                Some(RecordValue::PublicKey(pk)) => pk == public_key,
                _ => false,
            }
    }
}

/// Performs a match on a property to determine if it is a public key and matches the provided
/// public key
fn map_to_reference<'a>(
    record: &'a RecordRoot,
) -> impl Fn(&Property) -> Option<(FieldPath, Reference<'a>)> {
    move |p| match record.get_path(&p.path)? {
        RecordValue::ForeignRecordReference(record_ref) => {
            Some((p.path.clone(), Reference::ForeignRecord(record_ref)))
        }
        RecordValue::RecordReference(record) => Some((p.path.clone(), Reference::Record(record))),
        _ => None,
    }
}

// Filter iter for unique values
fn unique_filter<I, F, T, U>(iter: I, uniqueness_test: F) -> impl Iterator<Item = T>
where
    I: Iterator<Item = T>,
    F: Fn(&T) -> &U,
    U: Eq + Hash + PartialEq + Clone + 'static,
{
    let mut seen = HashSet::new();
    iter.filter(move |item| {
        let v = uniqueness_test(item);
        if seen.contains(v) {
            false
        } else {
            seen.insert(v.clone());
            true
        }
    })
}
#[cfg(test)]
mod test {
    use crate::record::RecordReference;

    use super::*;
    use polylang::parse;

    fn create_schema(collection_id: &str, code: &str) -> Schema {
        let mut program = None;
        let (_, ast) = parse(code, collection_id, &mut program).unwrap();
        #[allow(clippy::expect_used)]
        let collection_ast = ast
            .0
            .into_iter()
            .find_map(|node| match node {
                polylang::stableast::RootNode::Collection(collection) => Some(collection),
                _ => None,
            })
            .expect("No collection found");
        Schema::new(&collection_ast)
    }

    #[test]
    fn test_read_all() {
        let code = r#"
            @read
            collection Test {
                id: string;
            }
        "#;
        let schema = create_schema("Test", code);
        assert!(schema.read_all, "read_all should be true");
        assert!(!schema.call_all, "call_all should be false");
    }

    #[test]
    fn test_call_all() {
        let code = r#"
            @call
            collection Test {
                id: string;
            }
        "#;
        let schema = create_schema("Test", code);
        assert!(!schema.read_all, "read_all should be false");
        assert!(schema.call_all, "call_all should be true");
    }

    #[test]
    fn test_public_all() {
        let code = r#"
            @public
            collection Test {
                id: string;
            }
        "#;
        let schema = create_schema("Test", code);
        assert!(schema.read_all, "read_all should be true");
        assert!(schema.call_all, "call_all should be true");
    }

    #[test]
    fn test_fields_auth() {
        let code = r#"
            collection Test {
                id: string;
                @read
                name: PublicKey;
                @read
                ignored: string;
                @delegate
                delegate: Test;
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .fields_auth(&[DirectiveKind::Read, DirectiveKind::Delegate])
                .map(|p| p.path.to_string())
                .collect::<Vec<_>>(),
            vec!["name", "delegate"]
        );
    }

    #[test]
    fn test_find_directive_references() {
        let code = r#"
            collection Test {
                id: string;
                @read
                name: PublicKey;
                @read
                ignored: string;
                @read
                read: Test;
                @delegate
                delegate: Test;
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .find_directive_references(
                    &[DirectiveKind::Read, DirectiveKind::Delegate],
                    &RecordRoot(
                        [(
                            "read".to_string(),
                            RecordValue::RecordReference(RecordReference {
                                id: "test".to_string()
                            })
                        )]
                        .into()
                    )
                )
                .map(|(p, _)| p.to_string())
                .collect::<Vec<_>>(),
            vec!["read"],
        );
    }

    #[test]
    fn test_find_directive_references_empty_record() {
        let code = r#"
            collection Test {
                id: string;
                @read
                name: PublicKey;
                @read
                ignored: string;
                @read
                read: Test;
                @delegate
                delegate: Test;
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .find_directive_references(
                    &[DirectiveKind::Read, DirectiveKind::Delegate],
                    &RecordRoot(HashMap::new())
                )
                .map(|(p, _)| p.to_string())
                .collect::<Vec<_>>(),
            Vec::<String>::new(),
        );
    }

    #[test]
    fn test_method_auth() {
        let code = r#"
            collection Test {
                id: string;
                name: PublicKey;
                ignored: string;
                @delegate
                delegate: Test;
                @call
                call_prop: Test;

                @call(name)
                @call(call_prop)
                @call(ignored)
                function test() {}
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .method_auth("test")
                .map(|p| p.path.to_string())
                .collect::<Vec<_>>(),
            vec!["name", "call_prop"]
        );
    }

    #[test]
    fn test_method_references_with_empty_record() {
        let code = r#"
            collection Test {
                id: string;
                name: PublicKey;
                ignored: string;
                @delegate
                delegate: Test;
                @call
                call_prop: Test;

                @call(name)
                @call(call_prop)
                @call(ignored)
                function test() {}
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .find_method_references("test", &RecordRoot(HashMap::new()))
                .map(|(p, _)| p.to_string())
                .collect::<Vec<_>>(),
            Vec::<String>::new(),
        );
    }

    #[test]
    fn test_method_references_with_record() {
        let code = r#"
            collection Test {
                id: string;
                name: PublicKey;
                ignored: string;
                @delegate
                delegate: Test;
                @call
                call_prop: Test;

                @call(name)
                @call(call_prop)
                @call(ignored)
                function test() {}
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .find_method_references(
                    "test",
                    &RecordRoot(
                        [(
                            "call_prop".to_string(),
                            RecordValue::RecordReference(RecordReference {
                                id: "test".to_string()
                            })
                        )]
                        .into()
                    )
                )
                .map(|(p, _)| p.to_string())
                .collect::<Vec<_>>(),
            vec!["call_prop"]
        );
    }
}

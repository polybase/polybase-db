use super::{error::Result, field_path::FieldPath, index::Index, property::PropertyList, util};
use polylang::stableast;

// TODO: can we remove Clone
#[derive(Debug, Clone)]
pub struct Schema {
    id: String,
    /// List of indexes in the Collection
    /// Does this include custom AND field indexes?
    pub indexes: Vec<Index>,
    /// List of properties in the Collection
    pub properties: PropertyList,
    /// Anyone can read the collection.
    pub read_all: bool,
    /// Anyone can call the collection functions.
    pub call_all: bool,
}

impl Schema {
    pub fn new(id: &str, collection_ast: stableast::Collection) -> Result<Self> {
        let indexes = Index::from_ast(&collection_ast);

        let is_public = collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "public"));
        let read_all = is_public || collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "read" && d.arguments.is_empty()));
        let call_all = is_public || collection_ast.attributes.iter().any(|attr| matches!(attr, stableast::CollectionAttribute::Directive(d) if d.name == "call" && d.arguments.is_empty()));

        Ok(Self {
            id: id.to_string(),
            indexes,
            read_all,
            call_all,
            properties: PropertyList::from_ast_collection(&collection_ast),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> String {
        util::normalize_name(&self.id)
    }

    pub fn namespace(&self) -> &str {
        let Some(slash_index) = self.id.rfind('/') else {
            return "";
        };

        &self.id[0..slash_index]
    }

    pub fn validate(&self) -> Result<()> {
        // We don't need to validate the in-built collection as we know this will be valid
        if self.id() == "Collection" {
            return Ok(());
        }

        // TODO: move to here from validation in indexer_db_adaptor

        Ok(())
    }

    /// PublicKeys/Delegates in this list can read the collection
    // TODO: only PublicKey and RecordRefs are allowed here, do we need to check this?
    pub fn read_fields(&self) -> impl Iterator<Item = &FieldPath> {
        self.properties.iter().filter_map(|prop| {
            match prop.directives.iter().any(|dir| dir.name == "read") {
                true => Some(&prop.path),
                false => None,
            }
        })
    }

    /// PublicKeys/Delegates in this list have delegate permissions,
    /// i.e. if someone @read's a field with a record from this collection,
    /// anyone in the delegate list can read that record.
    // TODO: only PublicKey and RecordRefs are allowed here, do we need to check this?
    pub fn delegate_fields(&self) -> impl Iterator<Item = &FieldPath> {
        self.properties.iter().filter_map(|prop| {
            match prop.directives.iter().any(|dir| dir.name == "delegate") {
                true => Some(&prop.path),
                false => None,
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use polylang::parse;

    fn create_schema(collection_id: &str, code: &str) -> Schema {
        let mut program = None;
        let (_, ast) = parse(code, collection_id, &mut program).unwrap();
        let collection_ast = ast
            .0
            .into_iter()
            .find_map(|node| match node {
                polylang::stableast::RootNode::Collection(collection) => Some(collection),
                _ => None,
            })
            .expect("No collection found");
        Schema::new(collection_id, collection_ast).unwrap()
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
    fn test_property_read_directive() {
        let code = r#"
            collection Test {
                id: string;
                @read
                name: PublicKey;
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .read_fields()
                .map(|f| f.to_string())
                .collect::<Vec<_>>(),
            vec!["name"]
        );
    }

    #[test]
    fn test_property_delegate_directive() {
        let code = r#"
            collection Test {
                id: string;
                @delegate
                name: PublicKey;
            }
        "#;

        let schema = create_schema("Test", code);
        assert_eq!(
            schema
                .delegate_fields()
                .map(|f| f.to_string())
                .collect::<Vec<_>>(),
            vec!["name"]
        );
    }
}

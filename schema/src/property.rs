use super::{directive::Directive, field_path::FieldPath, types::Type};
use polylang::stableast;

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyList {
    properties: Vec<Property>,
}

impl PropertyList {
    pub fn from_ast_collection(ast: &stableast::Collection) -> Self {
        let properties = properties_from_ast(ast)
            .map(Property::from_ast_property)
            .collect();
        Self { properties }
    }

    pub fn from_ast_object(ast: &stableast::Object, parent: &FieldPath) -> Self {
        let properties = ast
            .fields
            .iter()
            .map(|p| Property::from_ast_object_field(p, parent))
            .collect();
        Self { properties }
    }

    /// Iterate through the top-level fields of a PropertyList
    pub fn iter(&self) -> impl Iterator<Item = &Property> {
        self.properties.iter()
    }

    /// Iterate through all fields, including nested object fields
    pub fn iter_all(&self) -> PropertyListIterator {
        PropertyListIterator::new(self)
    }
}
pub struct PropertyListIterator<'a> {
    stack: Vec<std::slice::Iter<'a, Property>>,
}

impl<'a> PropertyListIterator<'a> {
    fn new(property_list: &'a PropertyList) -> Self {
        Self {
            stack: vec![property_list.properties.iter()],
        }
    }
}

impl<'a> Iterator for PropertyListIterator<'a> {
    type Item = &'a Property;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.stack.last_mut() {
                if let Some(property) = iter.next() {
                    if let Type::Object(obj) = &property.type_ {
                        self.stack.push(obj.fields.properties.iter());
                    }
                    return Some(property);
                } else {
                    self.stack.pop();
                }
            } else {
                return None;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Property {
    pub path: FieldPath,
    pub type_: Type,
    pub required: bool,
    pub index: bool,
    pub directives: Vec<Directive>,
}

impl Property {
    fn from_ast_property(ast: &stableast::Property) -> Self {
        let stableast::Property {
            name,
            type_,
            required,
            directives,
        } = ast;
        let path = FieldPath::new(vec![name.to_string()]);
        let type_ = Type::from_ast(type_, &path);
        Self {
            path,
            required: *required,
            index: type_.is_indexable(),
            type_,
            directives: directives
                .iter()
                .map(Directive::from_ast_directive)
                .collect(),
        }
    }

    fn from_ast_object_field(ast: &stableast::ObjectField, parent: &FieldPath) -> Self {
        // TODO: allow directves on object fields
        let stableast::ObjectField {
            name,
            type_,
            required,
        } = ast;
        let path = parent.append(name.to_string());
        let type_ = Type::from_ast(type_, &path);
        Self {
            path,
            required: *required,
            index: false,
            type_,
            directives: vec![],
        }
    }
}

pub fn properties_from_ast<'a>(
    collection_ast: &'a stableast::Collection<'a>,
) -> impl Iterator<Item = &stableast::Property<'a>> {
    collection_ast.attributes.iter().filter_map(|a| match a {
        polylang::stableast::CollectionAttribute::Property(p) => Some(p),
        _ => None,
    })
}

use polylang::stableast;

pub enum Field<'ast> {
    /// A top level property.
    Property(&'ast stableast::Property<'ast>),
    /// A field of an object.
    ObjectField(&'ast stableast::ObjectField<'ast>),
}

impl<'ast> Field<'ast> {
    pub fn type_(&self) -> &stableast::Type<'ast> {
        match self {
            Field::Property(p) => &p.type_,
            Field::ObjectField(f) => &f.type_,
        }
    }
}

pub trait FieldWalker<'ast> {
    fn walk_fields(
        &'ast self,
        path: &mut Vec<&'ast str>,
        f: &mut impl FnMut(&[&'ast str], Field<'ast>),
    );
}

impl<'ast> FieldWalker<'ast> for stableast::Collection<'ast> {
    fn walk_fields(
        &'ast self,
        path: &mut Vec<&'ast str>,
        f: &mut impl FnMut(&[&'ast str], Field<'ast>),
    ) {
        for prop in self.attributes.iter().filter_map(|attr| match attr {
            stableast::CollectionAttribute::Property(p) => Some(p),
            _ => None,
        }) {
            path.push(prop.name.as_ref());
            f(path, Field::Property(prop));
            prop.type_.walk_fields(path, f);
            path.pop();
        }
    }
}

impl<'ast> FieldWalker<'ast> for stableast::Type<'ast> {
    fn walk_fields(
        &'ast self,
        path: &mut Vec<&'ast str>,
        f: &mut impl FnMut(&[&'ast str], Field<'ast>),
    ) {
        if let stableast::Type::Object(o) = self {
            for field in &o.fields {
                path.push(field.name.as_ref());
                f(path, Field::ObjectField(field));
                path.pop();
            }
        }
    }
}

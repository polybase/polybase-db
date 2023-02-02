use polylang::stableast;

pub(crate) trait FieldWalker<'ast> {
    fn walk_fields(
        &'ast self,
        path: &mut Vec<&'ast str>,
        f: &mut impl FnMut(&[&'ast str], &stableast::Type<'ast>),
    );
}

impl<'ast> FieldWalker<'ast> for stableast::Collection<'ast> {
    fn walk_fields(
        &'ast self,
        path: &mut Vec<&'ast str>,
        f: &mut impl FnMut(&[&'ast str], &stableast::Type<'ast>),
    ) {
        for prop in self.attributes.iter().filter_map(|attr| match attr {
            stableast::CollectionAttribute::Property(p) => Some(p),
            _ => None,
        }) {
            path.push(prop.name.as_ref());
            f(path, &prop.type_);
            prop.type_.walk_fields(path, f);
            path.pop();
        }
    }
}

impl<'ast> FieldWalker<'ast> for stableast::Type<'ast> {
    fn walk_fields(
        &'ast self,
        path: &mut Vec<&'ast str>,
        f: &mut impl FnMut(&[&'ast str], &stableast::Type<'ast>),
    ) {
        if let stableast::Type::Object(o) = self {
            for field in &o.fields {
                path.push(field.name.as_ref());
                f(path, &field.type_);
                path.pop();
            }
        }
    }
}

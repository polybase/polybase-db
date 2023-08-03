use polylang::stableast;

use super::field_path::FieldPath;

#[derive(Debug, PartialEq, Clone)]
pub struct Directive {
    // TODO: this should be an enum: e.g. Delegate, Read, Call, etc.
    pub name: String,
    pub arguments: Vec<FieldPath>,
}

impl Directive {
    /// Create a Directive from a stableast::Directive
    pub fn from_ast_directive(dir: &stableast::Directive) -> Self {
        let stableast::Directive { name, arguments } = dir;
        let arguments = arguments
            .iter()
            .filter_map(|arg| match arg {
                stableast::DirectiveArgument::FieldReference(field_ref) => Some(FieldPath::new(
                    field_ref.path.iter().map(|p| p.to_string()).collect(),
                )),
                stableast::DirectiveArgument::Unknown => None,
            })
            .collect();
        Self {
            name: name.to_string(),
            arguments,
        }
    }
}

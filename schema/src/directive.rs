use super::field_path::FieldPath;
use polylang::stableast;
use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
pub struct Directive {
    // TODO: this should be an enum: e.g. Delegate, Read, Call, etc.
    pub kind: DirectiveKind,
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
            kind: match name.as_ref() {
                "delegate" => DirectiveKind::Delegate,
                "read" => DirectiveKind::Read,
                "call" => DirectiveKind::Call,
                "public" => DirectiveKind::Public,
                _ => DirectiveKind::Unknown,
            },
            arguments,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DirectiveKind {
    Delegate,
    Read,
    Call,
    Public,
    Unknown,
}

impl DirectiveKind {
    pub fn allow_root(&self) -> bool {
        matches!(
            self,
            DirectiveKind::Read | DirectiveKind::Call | DirectiveKind::Public
        )
    }
}

impl Display for DirectiveKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirectiveKind::Delegate => write!(f, "delegate"),
            DirectiveKind::Read => write!(f, "read"),
            DirectiveKind::Call => write!(f, "call"),
            DirectiveKind::Public => write!(f, "public"),
            DirectiveKind::Unknown => write!(f, "unknown"),
        }
    }
}

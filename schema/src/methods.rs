use crate::{directive::Directive, field_path::FieldPath, types::Type};
use polylang::stableast;

#[derive(Debug, Clone)]
pub struct Method {
    pub name: String,
    pub directives: Vec<Directive>,
    pub parameters: Vec<Parameter>,
    pub returns: Option<ReturnValue>,
}

impl Method {
    pub fn from_ast_method(method: &stableast::Method) -> Self {
        let stableast::Method {
            name, attributes, ..
        } = method;

        let mut directives = vec![];
        let mut parameters = vec![];
        let mut returns = None;

        for attribute in attributes.iter() {
            match attribute {
                stableast::MethodAttribute::ReturnValue(val) => {
                    returns = Some(ReturnValue::from_ast_return_value(val))
                }
                stableast::MethodAttribute::Directive(d) => {
                    directives.push(Directive::from_ast_directive(d))
                }
                stableast::MethodAttribute::Parameter(p) => {
                    parameters.push(Parameter::from_ast_parameter(p))
                }
                _ => {}
            }
        }

        Self {
            name: name.to_string(),
            directives,
            parameters,
            returns,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: String,
    pub type_: Type,
    pub required: bool,
}

impl Parameter {
    pub fn from_ast_parameter(parameter: &stableast::Parameter) -> Self {
        let stableast::Parameter {
            name,
            type_,
            required,
        } = parameter;
        Self {
            name: name.to_string(),
            // TODO: maybe we shouldn't require a field path here
            type_: Type::from_ast(type_, &FieldPath::new(vec![])),
            required: *required,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReturnValue {
    type_: Type,
}

impl ReturnValue {
    pub fn from_ast_return_value(return_value: &stableast::ReturnValue) -> Self {
        let stableast::ReturnValue { type_, .. } = return_value;
        Self {
            type_: Type::from_ast(type_, &FieldPath::new(vec![])),
        }
    }
}

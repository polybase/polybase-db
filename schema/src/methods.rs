use crate::{
    directive::Directive,
    field_path::FieldPath,
    record::{Converter, RecordError, RecordValue},
    types::Type,
};
use polylang::stableast;
use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, UserError>;

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error("method {method_name} args invalid, expected {expected} got {actual}")]
    MethodIncorrectNumberOfArguments {
        method_name: String,
        expected: usize,
        actual: usize,
    },

    #[error("invalid argument type for parameter {parameter_name:?}: {source}")]
    MethodInvalidArgumentType {
        parameter_name: String,
        source: RecordError,
    },
}

#[derive(Debug, Clone)]
pub struct Method {
    pub name: String,
    pub directives: Vec<Directive>,
    pub parameters: Vec<Parameter>,
    pub returns: Option<ReturnValue>,
    pub code: String,
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
            code: method.code.to_string(),
        }
    }

    pub fn validate_args(&self, args: Vec<RecordValue>) -> Result<()> {
        let required_args_len = self.parameters.iter().filter(|p| p.required).count();
        if args.len() < required_args_len {
            return Err(UserError::MethodIncorrectNumberOfArguments {
                method_name: self.name.clone(),
                expected: required_args_len,
                actual: args.len(),
            })?;
        }

        if args.len() > self.parameters.len() {
            return Err(UserError::MethodIncorrectNumberOfArguments {
                method_name: self.name.clone(),
                expected: self.parameters.len(),
                actual: args.len(),
            })?;
        }

        // TODO: validate the arg values against the schema

        Ok(())
    }

    pub fn args_from_json(&self, args: &[serde_json::Value]) -> Result<Vec<RecordValue>> {
        self.parameters
            .iter()
            .zip(args.iter())
            .map(|(param, arg)| {
                if !param.required & arg.is_null() {
                    return Ok(RecordValue::Null);
                }

                Converter::convert(
                    (&param.type_, arg.clone()),
                    &mut vec![Cow::Borrowed(&param.name)],
                    false,
                )
                .map_err(|e| UserError::MethodInvalidArgumentType {
                    parameter_name: param.name.to_string(),
                    source: e,
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()
    }

    pub fn generate_js(&self) -> String {
        let parameters = self
            .parameters
            .iter()
            .map(|p| p.name.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        format!(
            "function {} ({}) {{\n{}\n}}",
            self.name, parameters, &self.code,
        )
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
    pub type_: Type,
}

impl ReturnValue {
    pub fn from_ast_return_value(return_value: &stableast::ReturnValue) -> Self {
        let stableast::ReturnValue { type_, .. } = return_value;
        Self {
            type_: Type::from_ast(type_, &FieldPath::new(vec![])),
        }
    }
}

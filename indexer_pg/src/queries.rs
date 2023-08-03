use super::util::strip_invalid_chars;
use indexer_db_adaptor::collection::record::RecordValue;
use polylang::stableast::{
    Primitive as ASTPrimitive, PrimitiveType as ASTPrimitiveType, Property as ASTProperty,
    Type as ASTType,
};
use std::{
    borrow::Cow,
    fmt::{self, Display, Formatter},
};

pub struct PgField<'a> {
    name: Cow<'a, str>,
    type_: PgType,
    required: bool,
    index: bool,
}

impl PgField<'_> {
    fn to_index(&self) -> PgIndex {
        PgIndex(vec![PgIndexField {
            field: self.name.clone(),
            direction: PgIndexDirectionType::Asc,
        }])
    }
}

pub struct PgIndex<'a>(Vec<PgIndexField<'a>>);

impl<'a> PgIndex<'a> {
    pub fn name(&self, table_name: &str) -> String {
        format!(
            "{}_{}",
            strip_invalid_chars(table_name),
            self.0
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join("_"),
        )
    }

    pub fn create(&self, table_name: &str) -> String {
        format!(
            "CREATE INDEX {} ON {} ({})",
            self.name(table_name),
            table_name,
            self.0
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    pub fn drop(&self, table_name: &str) -> String {
        format!("DROP INDEX IF EXISTS {}", self.name(table_name))
    }
}

pub struct PgIndexField<'a> {
    pub field: Cow<'a, str>,
    pub direction: PgIndexDirectionType,
}

impl Display for PgIndexField<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field, self.direction)
    }
}

pub enum PgIndexDirectionType {
    Asc,
    Desc,
}

impl Display for PgIndexDirectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PgIndexDirectionType::Asc => write!(f, "ASC"),
            PgIndexDirectionType::Desc => write!(f, "DESC"),
        }
    }
}

pub enum PgType {
    Text,
    Float,
    Boolean,
    Timestamp,
    Json,
    Bytes,
}

impl PgType {
    fn default(&self) -> String {
        match &self {
            PgType::Text => "''".to_string(),
            PgType::Float => "0.0".to_string(),
            PgType::Boolean => "false".to_string(),
            PgType::Timestamp => "NOW()".to_string(),
            PgType::Json => "'{}'".to_string(),
            PgType::Bytes => "'\\x'".to_string(),
        }
    }
}

impl Display for PgType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PgType::Text => write!(f, "TEXT"),
            PgType::Float => write!(f, "FLOAT"),
            PgType::Boolean => write!(f, "BOOLEAN"),
            PgType::Timestamp => write!(f, "TIMESTAMP"),
            PgType::Bytes => write!(f, "BYTEA"),
            PgType::Json => write!(f, "JSON"),
        }
    }
}

pub fn ast_to_pg_type(value: &ASTType) -> PgType {
    match value {
        ASTType::Primitive(ASTPrimitive { value }) => match value {
            ASTPrimitiveType::String => PgType::Text,
            ASTPrimitiveType::Number => PgType::Float,
            ASTPrimitiveType::Boolean => PgType::Boolean,
            ASTPrimitiveType::Bytes => PgType::Bytes,
        },
        ASTType::Array(_) => PgType::Json,
        ASTType::Map(_) => PgType::Json,
        ASTType::Object(_) => PgType::Json,
        ASTType::Record(_) => PgType::Json,
        ASTType::ForeignRecord(_) => PgType::Json,
        ASTType::PublicKey(_) => PgType::Json,
        ASTType::Unknown => PgType::Json,
    }
}

pub fn ast_to_pg_index(value: &ASTType) -> bool {
    matches!(
        value,
        ASTType::Primitive(_)
            | ASTType::Record(_)
            | ASTType::ForeignRecord(_)
            | ASTType::PublicKey(_)
    )
}

pub fn ast_to_pg_field(value: ASTProperty) -> PgField {
    PgField {
        name: value.name,
        type_: ast_to_pg_type(&value.type_),
        required: value.required,
        index: ast_to_pg_index(&value.type_),
    }
}

// pub fn record_value_to_pg_value(value: &RecordValue) ->

fn create_table(table_name: &str, fields: Vec<PgField>, indexes: Vec<PgIndex>) -> String {
    let fields_sql = fields
        .iter()
        .map(|value| {
            format!(
                "{} {}{}",
                strip_invalid_chars(&value.name),
                value.type_,
                match value.required {
                    true =>
                        if &value.name == "id" {
                            " PRIMARY KEY"
                        } else {
                            " NOT NULL"
                        },
                    false => "",
                }
            )
        })
        .collect::<Vec<String>>()
        .join(", ");

    // Add automatic field indexes
    let indexes_field_sql = fields
        .iter()
        .filter(|value| value.index)
        .map(|value| create_index(table_name, value.to_index()));

    // Add custom indexes
    let indexes_index_sql = indexes.iter().map(|index| index.create(table_name));

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}); {}",
        strip_invalid_chars(table_name),
        fields_sql,
        indexes_field_sql
            .chain(indexes_index_sql)
            .collect::<Vec<String>>()
            .join(", "),
    )
}

fn add_column(table_name: &str, column: PgField) -> String {
    let add_column = format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}{}",
        strip_invalid_chars(table_name),
        strip_invalid_chars(&column.name),
        column.type_,
        match column.required {
            // Add default value if we're adding a new required field
            // in case there are existing records
            true => format!(
                " NOT NULL DEFAULT {}::{}",
                column.type_.default(),
                column.type_
            ),
            false => "".to_string(),
        }
    );

    // Add index if we need it
    if column.index {
        format!(
            "{}; {}",
            add_column,
            create_index(table_name, column.to_index())
        )
    } else {
        add_column
    }
}

fn drop_column(table_name: &str, field_name: &str) -> String {
    // We don't need to drop the index, as Pg will do that for us
    // when we delete the column
    format!(
        "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
        strip_invalid_chars(table_name),
        strip_invalid_chars(field_name)
    )
}

fn create_index(table_name: &str, index: PgIndex) -> String {
    index.create(table_name)
}

fn drop_index(table_name: &str, index: PgIndex) -> String {
    index.drop(table_name)
}

fn alter_column_optional(table_name: &str, field: &str) -> String {
    format!(
        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
        strip_invalid_chars(table_name),
        strip_invalid_chars(field),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_with_id() {
        let table_name = "test_table";
        let query = create_table(
            table_name,
            vec![PgField {
                name: "id".into(),
                type_: PgType::Text,
                required: true,
                index: false,
            }],
            vec![],
        );

        assert_eq!(
            query,
            "CREATE TABLE IF NOT EXISTS test_table (id TEXT PRIMARY KEY)"
        );
    }

    #[test]
    fn test_create_table_with_multiple_fields() {
        let table_name = "test_table";
        let query = create_table(
            table_name,
            vec![
                PgField {
                    name: "id".into(),
                    type_: PgType::Text,
                    required: true,
                    index: true,
                },
                PgField {
                    name: "name".into(),
                    type_: PgType::Text,
                    required: false,
                    index: true,
                },
                PgField {
                    name: "age".into(),
                    type_: PgType::Float,
                    required: true,
                    index: true,
                },
                PgField {
                    name: "data".into(),
                    type_: PgType::Json,
                    required: false,
                    index: false,
                },
            ],
            vec![],
        );

        assert_eq!(
            query,
            "CREATE TABLE IF NOT EXISTS test_table (id TEXT PRIMARY KEY, name TEXT, age FLOAT NOT NULL, data JSON);
            CREATE INDEX ON test_table (id); CREATE INDEX ON test_table (name); CREATE INDEX ON test_table (age)"
            .split(';').map(|s|s.trim()).collect::<Vec<&str>>().join("; ")
        );
    }
}

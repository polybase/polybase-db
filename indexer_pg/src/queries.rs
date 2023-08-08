use super::{pg_type::schema_type_to_pg_type, util::strip_invalid_chars};
use schema::{index::Index, property::Property, Schema};

fn create_table(table_name: &str, schema: &Schema) -> String {
    let fields_sql = schema
        .properties
        .iter_all()
        .map(|prop| {
            format!(
                "{} {}{}",
                // TODO: should we add the clean path to the field_path type
                prop.path
                    .iter()
                    .map(strip_invalid_chars)
                    .collect::<Vec<String>>()
                    .join("."),
                schema_type_to_pg_type(&prop.type_),
                match prop.required {
                    true =>
                        if prop.path.is_id() {
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
    let indexes_sql = schema
        .indexes
        .iter()
        .map(|index| create_index(table_name, index));

    if indexes_sql.len() == 0 {
        return format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            strip_invalid_chars(table_name),
            fields_sql,
        );
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}); {}",
        strip_invalid_chars(table_name),
        fields_sql,
        indexes_sql.collect::<Vec<String>>().join("; "),
    )
}

fn add_column(table_name: &str, column: &Property) -> String {
    let col_type = schema_type_to_pg_type(&column.type_);
    let add_column = format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}{}",
        strip_invalid_chars(table_name),
        strip_invalid_chars(&column.path.to_string()),
        &col_type,
        match column.required {
            // Add default value if we're adding a new required field
            // in case there are existing records
            true => format!(" NOT NULL DEFAULT {}::{}", col_type.default(), &col_type),
            false => "".to_string(),
        }
    );

    // Add index if we need it
    if column.is_indexable() {
        format!(
            "{}; {}",
            add_column,
            create_index(table_name, &column.auto_index()),
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

fn index_name(table_name: &str, index: &Index) -> String {
    format!(
        "{}_{}",
        strip_invalid_chars(table_name),
        index
            .iter()
            .map(|f| format!("{}_{}", f.path, f.direction))
            .collect::<Vec<String>>()
            .join("_"),
    )
}

fn create_index(table_name: &str, index: &Index) -> String {
    let index_name = index_name(table_name, index);

    format!(
        "CREATE INDEX {} ON {} ({})",
        index_name,
        table_name,
        index
            .iter()
            .map(|f| format!(
                r#""{}" {}"#,
                strip_invalid_chars(&f.path.to_string()),
                f.direction
            ))
            .collect::<Vec<String>>()
            .join(", ")
    )
}

fn drop_index(table_name: &str, index: &Index) -> String {
    let index_name = index_name(table_name, index);
    format!("DROP INDEX IF EXISTS {}", index_name)
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
    use schema::{
        field_path::FieldPath,
        index::IndexField,
        property::{Property, PropertyList},
        types::{PrimitiveType, Type},
    };

    #[test]
    fn test_create_table_with_id() {
        let table_name = "test_table";
        let query = create_table(
            table_name,
            &Schema {
                name: table_name.to_string(),
                properties: PropertyList::new(vec![Property {
                    path: FieldPath::id(),
                    type_: Type::Primitive(PrimitiveType::String),
                    required: true,
                    index: true,
                    directives: vec![],
                }]),
                ..Schema::default()
            },
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
            &Schema {
                properties: PropertyList::new(vec![
                    Property {
                        path: FieldPath::id(),
                        type_: Type::Primitive(PrimitiveType::String),
                        required: true,
                        index: true,
                        directives: vec![],
                    },
                    Property {
                        path: FieldPath::new_single("name".to_string()),
                        type_: Type::Primitive(PrimitiveType::String),
                        required: false,
                        index: true,
                        directives: vec![],
                    },
                    Property {
                        path: FieldPath::new_single("age".to_string()),
                        type_: Type::Primitive(PrimitiveType::Number),
                        required: true,
                        index: true,
                        directives: vec![],
                    },
                    Property {
                        path: FieldPath::new_single("pk".to_string()),
                        type_: Type::PublicKey,
                        required: false,
                        index: false,
                        directives: vec![],
                    },
                ]),
                indexes: vec![
                    Index {
                        fields: vec![IndexField::new_asc(FieldPath::new_single("id".to_string()))],
                    },
                    Index {
                        fields: vec![IndexField::new_asc(FieldPath::new_single(
                            "name".to_string(),
                        ))],
                    },
                    Index {
                        fields: vec![IndexField::new_desc(FieldPath::new_single(
                            "age".to_string(),
                        ))],
                    },
                ],
                ..Schema::default()
            },
        );

        assert_eq!(
            query,
            r#"CREATE TABLE IF NOT EXISTS test_table (id TEXT PRIMARY KEY, name TEXT, age FLOAT NOT NULL, pk JSON);
            CREATE INDEX test_table_id_ASC ON test_table ("id" ASC); CREATE INDEX test_table_name_ASC ON test_table ("name" ASC); CREATE INDEX test_table_age_DESC ON test_table ("age" DESC)"#
            .split(';').map(|s|s.trim()).collect::<Vec<&str>>().join("; ")
        );
    }
}

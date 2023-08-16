use serde_json::json;

use crate::api::{Error, ErrorData, Server};

macro_rules! create_collection_test {
    ($error:expr, $test_name:ident, $collection_id:expr, $schema:expr, $signer:expr $(,)?) => {
        #[tokio::test]
        async fn $test_name() {
            let server = Server::setup_and_wait(None).await;

            let err = server
                .create_collection::<serde_json::Value>($collection_id, $schema, $signer)
                .await
                .unwrap_err();

            assert_eq!(err, $error);
        }
    };
    ($error:expr, $test_name:ident, $collection_id:expr, $schema:expr $(,)?) => {
        create_collection_test!($error, $test_name, $collection_id, $schema, None);
    };
}

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-id".to_string(),
            message: "collection id is missing namespace".to_string(),
        }
    },
    no_namespace,
    "test",
    "
@public
collection test {
    id: string;
    name: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-id".to_string(),
            message: "collection name cannot start with '$'".to_string(),
        },
    },
    collection_with_dollar_prefix,
    "test/$internal",
    "collection $internal { id: string; }",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "failed-precondition".to_string(),
            reason: "function/javascript-exception".to_string(),
            message: "JavaScript exception error: Error found at line 2, column 15: Unrecognized token \"-\". Expected one of: \"{\"\ncollection test-cities {\n               ^".to_string(),
        },
    },
    collection_with_dash,
    "test/test-cities",
    "
collection test-cities {
    id: string;
    name: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "code is missing definition for collection cities2".to_string(),
        }
    },
    missing_collection,
    "ns/cities2",
    "collection cities {}",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "collection must have an 'id' field".to_string(),
        }
    },
    collection_without_id,
    "ns/test",
    "
@public
collection test {
    name: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "collection 'id' field must be a string".to_string(),
        }
    },
    collection_with_id_as_number,
    "ns/test",
    "
@public
collection test {
    id: number;
    name: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "collection 'id' field cannot be optional".to_string(),
        }
    },
    collection_with_optional_id,
    "ns/test",
    "
@public
collection test {
    id?: string;
    name: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "cannot index field \"arr\" of type string[]".to_string(),
        }
    },
    collection_with_index_on_array_field,
    "ns/test",
    "
@public
collection test {
    id: string;
    arr: string[];

    @index(arr);

    constructor (id: string) {
        this.id = id;
    }
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "cannot index field \"more.arr\" of type string[]".to_string(),
        }
    },
    collection_with_index_on_nested_array_field,
    "ns/test",
    "
@public
collection test {
    id: string;
    more: {
        arr: string[];
    };

    @index(more.arr);

    constructor (id: string) {
        this.id = id;
    }
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "cannot index field \"m\" of type map<string, string>".to_string(),
        }
    },
    collection_with_index_on_map_field,
    "ns/test",
    "
@public
collection test {
    id: string;
    m: map<string, string>;

    @index(m);

    constructor (id: string) {
        this.id = id;
    }
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "cannot index field \"info\" of type { info.name: string; }".to_string(),
        }
    },
    collection_with_index_on_object_field,
    "ns/test",
    "
@public
collection test {
    id: string;
    info: {
        name: string;
    };

    @index(info);

    constructor (id: string) {
        this.id = id;
    }
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "cannot index field \"data\" of type bytes".to_string(),
        }
    },
    collection_with_index_on_bytes_field,
    "ns/test",
    "
@public
collection test {
    id: string;
    data: bytes;

    @index(data);

    constructor (id: string) {
        this.id = id;
    }
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: "index field \"id1\" not found in schema".to_string(),
        }
    },
    collection_with_invalid_index_name,
    "ns/test",
    "
     @public
     collection test {
        id: string;

        @index(id1);
     }
     ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: r#"collection directive "read" cannot have arguments"#.to_string(),
        }
    },
    collection_with_read_directive_with_arguments,
    "ns/test",
    "
@read(creator)
collection test {
    id: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "invalid-argument".to_string(),
            reason: "collection/invalid-schema".to_string(),
            message: r#"collection directive "call" cannot have arguments"#.to_string(),
        }
    },
    collection_with_call_directive_with_arguments,
    "ns/test",
    "
@call(creator)
collection test {
    id: string;
}
    ",
);

create_collection_test!(
    Error {
        error: ErrorData {
            code: "failed-precondition".to_string(),
            reason: "function/javascript-exception".to_string(),
            message: r#"JavaScript exception error: Error found at line 2, column 1: Unknown decorator: unknown
@unknown
 ^"#.to_string(),
        }
    },
    collection_with_unknown_directive_with_arguments,
    "ns/test",
    "
@unknown
collection test {
    id: string;
}
    ",
);

#[tokio::test]
async fn function_not_found() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/test",
            "
@public
collection test {
    id: string;
}
    ",
            None,
        )
        .await
        .unwrap();

    let err = collection
        .call("none", "test", json!([]), None)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        Error {
            error: ErrorData {
                code: "not-found".to_string(),
                reason: "function/not-found".to_string(),
                message: r#"method "test" not found in collection "ns/test""#.to_string(),
            }
        }
    );
}

#[tokio::test]
async fn constructor_does_not_assign_id() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/test",
            "
@public
collection test {
    id: string;

    constructor (id: string) {}
}
    ",
            None,
        )
        .await
        .unwrap();

    let err = collection.create(json!(["id"]), None).await.unwrap_err();

    assert_eq!(
        err,
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "constructor/no-id-assigned".to_string(),
                message: "constructor must assign id".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn constructor_does_not_assign_required() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/test",
            "
@public
collection test {
    id: string;
    arr: array[];

    constructor (id: string) {
        this.id = id;
    }
}
    ",
            None,
        )
        .await
        .unwrap();

    let err = collection.create(json!(["id"]), None).await.unwrap_err();

    assert_eq!(
        err,
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "record/missing-field".to_string(),
                message: "record is missing field \"arr\"".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn id_already_exists() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/test",
            "
@public
collection test {
    id: string;
    name?: string;

    constructor (id: string) {
        this.id = id;
    }
}
    ",
            None,
        )
        .await
        .unwrap();

    collection.create(json!(["id4"]), None).await.unwrap();

    let err = collection.create(json!(["id4"]), None).await.unwrap_err();

    assert_eq!(
        err,
        Error {
            error: ErrorData {
                code: "already-exists".to_string(),
                reason: "collection/id-exists".to_string(),
                message: "record id already exists in collection".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn id_invalidated() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/test",
            "
@public
collection test {
    id: string;

    constructor (id: string) {
        this.id = id;
    }

    update() {
        this.id = 'id2';
    }
}
    ",
            None,
        )
        .await
        .unwrap();

    collection.create(json!(["id4"]), None).await.unwrap();

    let err = collection
        .call("id4", "update", json!([]), None)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        Error {
            error: ErrorData {
                code: "failed-precondition".to_string(),
                reason: "record/id-modified".to_string(),
                message: "record ID was modified".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn record_already_exists() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/test",
            "
collection test {
    id: string;

    constructor (id: string) {
        this.id = id;
    }
}
            ",
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        collection.create(json!(["id4"]), None).await.unwrap(),
        json!({
            "id": "id4"
        }),
    );

    assert_eq!(
        collection.create(json!(["id4"]), None).await.unwrap_err(),
        Error {
            error: ErrorData {
                code: "already-exists".to_string(),
                reason: "collection/id-exists".to_string(),
                message: "record id already exists in collection".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn invalid_value_type() {
    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection_untyped(
            "ns/Test",
            r#"
@public
collection Test {
    id: string;
    name: string;
    extra: {
        surname: string;
        testRecord?: Test;
    };
    arr: string[];

    constructor (id: string, name: string) {
        this.id = id;
        this.name = name;
        this.extra = { surname: 'Doe' };
        this.arr = ['a', 'b'];
    }

    changeNameToUndefined() {
        this.name = undefined;
    }
    
    changeSurnameToUndefined() {
        this.extra.surname = undefined;
    }

    changeNameToNumber() {
        this.name = 1;
    }

    changeSurnameToNumber() {
        this.extra.surname = 1;
    }

    addNumberToArray() {
        this.arr.push(1);
    }

    changeNameAndSurnameToNumber() {
        this.name = 1;
        this.extra.surname = 1;
    }

    setRecord(test: Test) {
        this.extra.testRecord = test;
    }
}
        "#,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        collection
            .create(json!(["id1", "John"]), None)
            .await
            .unwrap(),
        json!({
            "id": "id1",
            "name": "John",
            "extra": {
                "surname": "Doe"
            },
            "arr": ["a", "b"]
        }),
    );

    assert_eq!(
        collection
            .call("id1", "changeNameToUndefined", json!([]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "record/missing-field".to_string(),
                message: "record is missing field \"name\"".to_string(),
            }
        }
    );

    assert_eq!(
        collection
            .call("id1", "changeSurnameToUndefined", json!([]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "record/missing-field".to_string(),
                message: "record is missing field \"extra.surname\"".to_string(),
            }
        }
    );

    // Removed the check on foreign reference additional fields, as it doesn't impact anything
    // as they are stripped out
    // assert_eq!(
    //     collection
    //         .call("id1", "setRecord", json!([json!({"collectionId": collection.id.clone(), "id": "id2", "extraFieldName": "John"})]), None)
    //         .await
    //         .unwrap_err(),
    //     Error {
    //         error: ErrorData {
    //             code: "invalid-argument".to_string(),
    //             reason: "function/invalid-args".to_string(),
    //             message: r#"invalid argument type for parameter "test": unexpected fields: test.extraFieldName"#.to_string(),
    //         }
    //     }
    // );

    assert_eq!(
        collection
            .call("id1", "setRecord", json!([json!({"collectionId": collection.id.clone(), "id": 123})]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "function/invalid-args".to_string(),
                message: r#"invalid argument type for parameter "test": record reference has invalid field type"#.to_string(),
            }
        }
    );
}

#[tokio::test]
async fn invalid_index_name_update() {
    let server = Server::setup_and_wait(None).await;

    server
        .create_collection_untyped(
            "ns/InvalidIndexTest",
            r#"
        @public
        collection InvalidIndexTest {
            id: string;

            constructor(id: string) {
                this.id = id;
            }

            @index(id);
        }
    "#,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        server
            .update_collection_untyped(
                "ns/InvalidIndexTest",
                r#"
        @public
        collection InvalidIndexTest {
            id: string;

            constructor(id: string) {
                this.id = id;
            }

            @index(id1);
        }
    "#,
                None
            )
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "collection/invalid-schema".to_string(),
                message: "index field \"id1\" not found in schema".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn change_schema_type() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
    @public
    collection Account {
        id: string;
    
        constructor (id: string) {
            this.id = id;
        }
    }"#;

    let col = server
        .create_collection_untyped("ns/Account", schema, None)
        .await
        .unwrap();

    col.create(json!(["id1"]), None).await.unwrap();

    let schema_with_col_ref = r#"
    @public
    collection Account {
        id: string;
        x: SomeCol;
    
        constructor (id: string) {
            this.id = id;
        }
    }
    "#;

    let col = server
        .update_collection_untyped("ns/Account", schema_with_col_ref, None)
        .await
        .unwrap();

    assert_eq!(
        col.get("id1", None).await.unwrap(),
        json!({
            "id": "id1",
            "x": {
                "collectionId": "",
                "id": "",
            },
        })
    );

    let schema_with_object = r#"
    @public
    collection Account {
        id: string;
        x: {
            name: string;
        };
    
        constructor (id: string) {
            this.id = id;
        }
    }"#;

    assert_eq!(
        server
            .update_collection_untyped("ns/Account", schema_with_object, None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "collection/invalid-schema".to_string(),
                message:
                    r#"cannot change type of fields: "x", delete the fields and re-create them"#
                        .to_string(),
            }
        }
    );
}

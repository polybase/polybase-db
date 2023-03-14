use serde_json::json;

use crate::api::{Error, ErrorData, Server};

macro_rules! create_collection_test {
    ($error:expr, $test_name:ident, $collection_id:expr, $schema:expr, $signer:expr $(,)?) => {
        #[tokio::test]
        async fn $test_name() {
            let server = Server::setup_and_wait().await;

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
            message: "cannot index field \"arr\" of type array".to_string(),
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
            message: "cannot index field \"more.arr\" of type array".to_string(),
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
            message: "cannot index field \"m\" of type map".to_string(),
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
            message: "cannot index field \"info\" of type object".to_string(),
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

#[tokio::test]
async fn function_not_found() {
    let server = Server::setup_and_wait().await;

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
    let server = Server::setup_and_wait().await;

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
async fn id_already_exists() {
    let server = Server::setup_and_wait().await;

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
    let server = Server::setup_and_wait().await;

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
    let server = Server::setup_and_wait().await;

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

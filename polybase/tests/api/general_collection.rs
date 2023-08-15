use serde::Deserialize;
use serde_json::json;

use crate::api::{ListQuery, Server};

#[tokio::test]
async fn test_collection() {
    let server = Server::setup_and_wait(None).await;

    #[derive(Debug, Deserialize, PartialEq)]
    struct User {
        id: String,
        name: Option<String>,
        age: Option<f64>,
    }

    let collection = server
        .create_collection::<User>(
            "ns/User",
            r#"
@public
collection User {
    id: string;
    name?: string;
    age?: number;

    @index(name);
    @index(age);

    constructor (id: string, name: string, age: number) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    updateAge (age: number) {
        this.age = age;
    }

    update (name: string, age: number) {
        this.name = name;
        this.age = age;
    }

    destroy () {
        selfdestruct();
    }
}
    "#,
            None,
        )
        .await
        .unwrap();

    let user_0_john_30 = User {
        id: "0".to_string(),
        name: Some("John".to_string()),
        age: Some(30.0),
    };

    let user_1_john_40 = User {
        id: "1".to_string(),
        name: Some("John".to_string()),
        age: Some(40.0),
    };

    let user_2_tom_30 = User {
        id: "2".to_string(),
        name: Some("Tom".to_string()),
        age: Some(30.0),
    };

    let user_3_last_john1_50 = User {
        id: "3/last".to_string(),
        name: Some("John1".to_string()),
        age: Some(50.0),
    };

    let user_4_joe_30 = User {
        id: "4".to_string(),
        name: Some("Joe".to_string()),
        age: Some(30.0),
    };

    let create = |user: &User| collection.create(json!([user.id, user.name, user.age]), None);

    assert_eq!(create(&user_0_john_30).await.unwrap(), user_0_john_30);
    assert_eq!(
        collection.get(&user_0_john_30.id, None).await.unwrap(),
        user_0_john_30
    );

    assert_eq!(create(&user_1_john_40).await.unwrap(), user_1_john_40);
    assert_eq!(
        collection.get(&user_1_john_40.id, None).await.unwrap(),
        user_1_john_40,
    );

    assert_eq!(create(&user_2_tom_30).await.unwrap(), user_2_tom_30);
    assert_eq!(
        collection.get(&user_2_tom_30.id, None).await.unwrap(),
        user_2_tom_30
    );

    assert_eq!(
        create(&user_3_last_john1_50).await.unwrap(),
        user_3_last_john1_50
    );
    assert_eq!(
        collection
            .get(&user_3_last_john1_50.id, None)
            .await
            .unwrap(),
        user_3_last_john1_50,
    );

    assert_eq!(create(&user_4_joe_30).await.unwrap(), user_4_joe_30);
    assert_eq!(
        collection.get(&user_4_joe_30.id, None).await.unwrap(),
        user_4_joe_30
    );

    // Update age of record 4 to 40
    let user_4_joe_40 = User {
        id: "4".to_string(),
        name: Some("Joe".to_string()),
        age: Some(40.0),
    };
    drop(user_4_joe_30);
    assert_eq!(
        collection
            .call(
                &user_4_joe_40.id,
                "updateAge",
                json!([user_4_joe_40.age]),
                None
            )
            .await
            .unwrap()
            .unwrap(),
        user_4_joe_40
    );

    // Delete record 4
    assert_eq!(
        collection
            .call(&user_4_joe_40.id, "destroy", json!([]), None)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        collection
            .get(&user_4_joe_40.id, None)
            .await
            .unwrap_err()
            .error
            .reason,
        "record/not-found"
    );

    // List records
    {
        let list_name_john = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "name": "John",
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_name_john.data.len(), 2);
        assert_eq!(list_name_john.data[0].data, user_0_john_30);
        assert_eq!(list_name_john.data[1].data, user_1_john_40);
    }
    {
        let list_age_30 = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "age": 30,
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_age_30.data.len(), 2);
        assert_eq!(list_age_30.data[0].data, user_0_john_30);
        assert_eq!(list_age_30.data[1].data, user_2_tom_30);
    }
    {
        let list_age_gt_30 = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "age": {
                            "$gt": 30,
                        },
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_age_gt_30.data.len(), 2);
        assert_eq!(list_age_gt_30.data[0].data, user_1_john_40);
        assert_eq!(list_age_gt_30.data[1].data, user_3_last_john1_50);
    }
    {
        let list_age_gte_30 = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "age": {
                            "$gte": 30,
                        },
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_age_gte_30.data.len(), 4);
        assert_eq!(list_age_gte_30.data[0].data, user_0_john_30);
        assert_eq!(list_age_gte_30.data[1].data, user_2_tom_30);
        assert_eq!(list_age_gte_30.data[2].data, user_1_john_40);
        assert_eq!(list_age_gte_30.data[3].data, user_3_last_john1_50);
    }
    {
        let list_age_lt_40 = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "age": {
                            "$lt": 40,
                        },
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_age_lt_40.data.len(), 2);
        assert_eq!(list_age_lt_40.data[0].data, user_0_john_30);
        assert_eq!(list_age_lt_40.data[1].data, user_2_tom_30);
    }
    {
        let list_age_lte_40 = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "age": {
                            "$lte": 40,
                        },
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_age_lte_40.data.len(), 3);
        assert_eq!(list_age_lte_40.data[0].data, user_0_john_30);
        assert_eq!(list_age_lte_40.data[1].data, user_2_tom_30);
        assert_eq!(list_age_lte_40.data[2].data, user_1_john_40);
    }
    {
        let list_age_gt_30_lt_41 = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "age": {
                            "$gt": 30,
                            "$lt": 41,
                        },
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_age_gt_30_lt_41.data.len(), 1);
        assert_eq!(list_age_gt_30_lt_41.data[0].data, user_1_john_40);
    }
    {
        let list_name_gt_john = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "name": {
                            "$gt": "John",
                        },
                    })),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(list_name_gt_john.data.len(), 2);
        assert_eq!(list_name_gt_john.data[0].data, user_3_last_john1_50);
        assert_eq!(list_name_gt_john.data[1].data, user_2_tom_30);
    }

    // Cursors
    {
        let mut all_records = vec![];
        let limit = 1;
        let mut after = None;
        let mut before = None;

        loop {
            let list = collection
                .list(
                    ListQuery {
                        limit: Some(limit),
                        after,
                        ..Default::default()
                    },
                    None,
                )
                .await
                .unwrap();

            if list.data.is_empty() {
                break;
            }

            all_records.extend(list.data.into_iter().map(|r| r.data));

            after = list.cursor.after;
            before = list.cursor.before;
        }

        println!("before: {:?}", before);

        assert_eq!(all_records.len(), 4);
        assert_eq!(all_records[0], user_0_john_30);
        assert_eq!(all_records[1], user_1_john_40);
        assert_eq!(all_records[2], user_2_tom_30);
        assert_eq!(all_records[3], user_3_last_john1_50);

        let mut all_records = vec![];
        loop {
            let list = collection
                .list(
                    ListQuery {
                        limit: Some(limit),
                        before: before.clone(),
                        ..Default::default()
                    },
                    None,
                )
                .await
                .unwrap();

            if list.data.is_empty() {
                break;
            }

            all_records.extend(list.data.into_iter().map(|r| r.data));
            before = list.cursor.before;
        }

        assert_eq!(all_records.len(), 4);
        assert_eq!(all_records[0], user_3_last_john1_50);
        assert_eq!(all_records[1], user_2_tom_30);
        assert_eq!(all_records[2], user_1_john_40);
        assert_eq!(all_records[3], user_0_john_30);
    }
}

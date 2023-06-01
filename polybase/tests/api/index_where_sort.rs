use serde_json::json;

use crate::api::{ListQuery, Server};

#[tokio::test]
async fn index_where_sort() {
    let schema = r#"
@public
collection PeopleIndexWhereSort { 
    id: string; 
    name?: string; 
    age?: number;
    place?: string; 

    constructor (id: string, name: string, age: number, place: string) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.place = place;
    }
    
    @index(name, [age, desc], place);
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct People {
        id: String,
        name: Option<String>,
        age: Option<f64>,
        place: Option<String>,
    }

    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection::<People>("test/PeopleIndexWhereSort", schema, None)
        .await
        .unwrap();

    let person_cal_30_uk = People {
        id: "0".to_string(),
        name: Some("cal".to_string()),
        age: Some(30.0),
        place: Some("UK".to_string()),
    };

    let person_cal2_40_uk = People {
        id: "1".to_string(),
        name: Some("cal2".to_string()),
        age: Some(40.0),
        place: Some("UK".to_string()),
    };

    let person_cal3_50_uk = People {
        id: "2".to_string(),
        name: Some("cal3".to_string()),
        age: Some(50.0),
        place: Some("UK".to_string()),
    };

    assert_eq!(
        collection
            .create(json!(["0", "cal", 30.0, "UK"]), None)
            .await
            .unwrap(),
        person_cal_30_uk
    );

    assert_eq!(
        collection
            .create(json!(["1", "cal2", 40.0, "UK"]), None)
            .await
            .unwrap(),
        person_cal2_40_uk
    );

    assert_eq!(
        collection
            .create(json!(["2", "cal3", 50.0, "UK"]), None)
            .await
            .unwrap(),
        person_cal3_50_uk
    );

    // List records with where + sort
    {
        let res = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "name": { "$gt": "cal" },
                    })),
                    sort: Some(json!([["name", "desc"]])),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.data.len(), 2);
        assert_eq!(res.data[0].data, person_cal3_50_uk);
        assert_eq!(res.data[1].data, person_cal2_40_uk);
    }
    {
        let res = collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "name": { "$gt": "cal" },
                    })),
                    sort: Some(json!([["name", "asc"]])),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.data.len(), 2);
        assert_eq!(res.data[0].data, person_cal2_40_uk);
        assert_eq!(res.data[1].data, person_cal3_50_uk);
    }
}

use serde_json::json;

use crate::api::Server;

#[tokio::test]
async fn collection_map_field() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Asset {
    id: string;
    accountToBalance: map<string, number>;
    balanceCounts: map<number, number>;
    accountAssetBalances: map<string, map<string, number>>;

    constructor (id: string, accountToBalance: map<string, number>, balanceCounts: map<number, number>, accountAssetBalances: map<string, map<string, number>>) {
        this.id = id;
        this.accountToBalance = accountToBalance;
        this.balanceCounts = balanceCounts;
        this.accountAssetBalances = accountAssetBalances;
    }

    transfer(from: string, to: string, amount: number) {
        this.balanceCounts[this.accountToBalance[from]] -= 1;
        this.balanceCounts[this.accountToBalance[to]] -= 1;

        this.accountToBalance[from] -= amount;
        this.accountToBalance[to] += amount;
        this.accountAssetBalances[from]['default'] -= amount;
        this.accountAssetBalances[to]['default'] += amount;

        if (this.balanceCounts[this.accountToBalance[from]]) {
            this.balanceCounts[this.accountToBalance[from]] += 1;
        } else {
            this.balanceCounts[this.accountToBalance[from]] = 1;
        }

        if (this.balanceCounts[this.accountToBalance[to]]) {
            this.balanceCounts[this.accountToBalance[to]] += 1;
        } else {
            this.balanceCounts[this.accountToBalance[to]] = 1;
        }
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Asset {
        id: String,
        account_to_balance: std::collections::HashMap<String, f64>,
        balance_counts: std::collections::HashMap<u64, f64>,
        account_asset_balances:
            std::collections::HashMap<String, std::collections::HashMap<String, f64>>,
    }

    let collection = server
        .create_collection::<Asset>("test/Asset", schema, None)
        .await
        .unwrap();

    assert_eq!(
        collection
            .create(
                json!([
                    "id1",
                    {
                        "acc1": 100.0,
                        "acc2": 200.0,
                    },
                    {
                        "100": 1.0,
                        "200": 1.0,
                    },
                    {
                        "acc1": {
                            "default": 100.0,
                        },
                        "acc2": {
                            "default": 200.0,
                        },
                    },
                ]),
                None
            )
            .await
            .unwrap(),
        Asset {
            id: "id1".to_string(),
            account_to_balance: {
                let mut map = std::collections::HashMap::new();
                map.insert("acc1".to_string(), 100.0);
                map.insert("acc2".to_string(), 200.0);
                map
            },
            balance_counts: {
                let mut map = std::collections::HashMap::new();
                map.insert(100, 1.0);
                map.insert(200, 1.0);
                map
            },
            account_asset_balances: {
                let mut map = std::collections::HashMap::new();
                map.insert("acc1".to_string(), {
                    let mut map = std::collections::HashMap::new();
                    map.insert("default".to_string(), 100.0);
                    map
                });
                map.insert("acc2".to_string(), {
                    let mut map = std::collections::HashMap::new();
                    map.insert("default".to_string(), 200.0);
                    map
                });
                map
            },
        }
    );

    assert_eq!(
        collection
            .call("id1", "transfer", json!(["acc1", "acc2", 50.0]), None)
            .await
            .unwrap()
            .unwrap(),
        Asset {
            id: "id1".to_string(),
            account_to_balance: {
                let mut map = std::collections::HashMap::new();
                map.insert("acc1".to_string(), 50.0);
                map.insert("acc2".to_string(), 250.0);
                map
            },
            balance_counts: {
                let mut map = std::collections::HashMap::new();
                map.insert(100, 0.0);
                map.insert(200, 0.0);
                map.insert(50, 1.0);
                map.insert(250, 1.0);
                map
            },
            account_asset_balances: {
                let mut map = std::collections::HashMap::new();
                map.insert("acc1".to_string(), {
                    let mut map = std::collections::HashMap::new();
                    map.insert("default".to_string(), 50.0);
                    map
                });
                map.insert("acc2".to_string(), {
                    let mut map = std::collections::HashMap::new();
                    map.insert("default".to_string(), 250.0);
                    map
                });
                map
            },
        }
    );
}

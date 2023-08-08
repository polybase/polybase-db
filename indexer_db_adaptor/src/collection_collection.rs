use super::ast::collection_ast_from_record;
use once_cell::sync::Lazy;
use schema::record::{RecordRoot, RecordValue};
use schema::Schema;
use std::collections::HashMap;

// pub static COLLECTION_SCHEMA: Lazy<Schema<'static>> = Lazy::new(|| {
//     let mut program = None;

//     #[allow(clippy::unwrap_used)]
//     let (_, stable_ast) = polylang::parse(&COLLECTION_CODE, "", &mut program).unwrap();

//     #[allow(clippy::unwrap_used)]
//     let collection_ast = collection_ast_from_root(stable_ast, "Collection").unwrap();

//     #[allow(clippy::unwrap_used)]
//     Schema::new("Collection", collection_ast).unwrap()
// });

pub fn get_collection_collection_schema() -> Schema {
    #[allow(clippy::unwrap_used)]
    let collection_ast: polylang::stableast::Collection<'_> =
        collection_ast_from_record(&COLLECTION_COLLECTION_RECORD, "Collection").unwrap();
    #[allow(clippy::unwrap_used)]
    Schema::new(&collection_ast)
}

pub static COLLECTION_CODE: Lazy<&'static str> = Lazy::new(|| {
    r#"
    @public
    collection Collection {
        id: string;
        name?: string;
        lastRecordUpdated?: string;
        code?: string;
        ast?: string;
        publicKey?: PublicKey;
    
        @index(publicKey);
        @index([lastRecordUpdated, desc]);
    
        constructor (id: string, code: string) {
            this.id = id;
            this.code = code;
            this.ast = parse(code, id);
            if (ctx.publicKey) this.publicKey = ctx.publicKey;
        }
    
        updateCode (code: string) {
            if (this.publicKey != ctx.publicKey) {
                throw error('invalid owner');
            }
            this.code = code;
            this.ast = parse(code, this.id);
        }
    }
    "#
});

pub static COLLECTION_COLLECTION_RECORD: Lazy<RecordRoot> = Lazy::new(|| {
    let mut hm = RecordRoot::new();

    hm.insert(
        "id".to_string(),
        RecordValue::String("Collection".to_string()),
    );

    let code = COLLECTION_CODE.clone();

    hm.insert(
        "code".to_string(),
        // The replaces are for clients <=0.3.23
        RecordValue::String(code.replace("@public", "").replace("PublicKey", "string")),
    );

    let mut program = None;
    #[allow(clippy::unwrap_used)]
    let (_, stable_ast) = polylang::parse(code, "", &mut program).unwrap();
    hm.insert(
        "ast".to_string(),
        #[allow(clippy::unwrap_used)]
        RecordValue::String(serde_json::to_string(&stable_ast).unwrap()),
    );

    hm
});

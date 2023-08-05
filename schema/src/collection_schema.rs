use super::ast::collection_ast_from_root;
use crate::Schema;
use once_cell::sync::Lazy;

pub static COLLECTION_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    let mut program = None;

    #[allow(clippy::unwrap_used)]
    let (_, stable_ast) = polylang::parse(&COLLECTION_CODE, "", &mut program).unwrap();

    #[allow(clippy::unwrap_used)]
    let collection_ast = collection_ast_from_root("Collection", stable_ast).unwrap();

    #[allow(clippy::unwrap_used)]
    Schema::new(&collection_ast)
});

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

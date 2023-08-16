use crate::{cursor::Cursor, where_query};
use schema::index;

pub struct ListQuery<'a> {
    pub limit: Option<usize>,
    pub where_query: where_query::WhereQuery<'a>,
    pub order_by: &'a [index::IndexField],
    pub cursor_before: Option<Cursor<'a>>,
    pub cursor_after: Option<Cursor<'a>>,
}

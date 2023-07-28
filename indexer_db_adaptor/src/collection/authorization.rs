use super::where_query;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Authorization {
    /// Anyone can read the collection.
    pub(crate) read_all: bool,
    /// Anyone can call the collection functions.
    pub(crate) call_all: bool,
    /// PublicKeys/Delegates in this list can read the collection.
    pub(crate) read_fields: Vec<where_query::FieldPath>,
    /// PublicKeys/Delegates in this list have delegate permissions,
    /// i.e. if someone @read's a field with a record from this collection,
    /// anyone in the delegate list can read that record.
    pub(crate) delegate_fields: Vec<where_query::FieldPath>,
}

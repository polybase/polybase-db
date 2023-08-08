pub fn normalize_name(collection_id: &str) -> String {
    #[allow(clippy::unwrap_used)] // split always returns at least one element
    let last_part = collection_id.split('/').last().unwrap();

    last_part.replace('-', "_")
}

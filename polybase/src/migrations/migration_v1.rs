use super::{async_trait, Migrate, Migration, MigrationResult};
use slog::info;

#[derive(Debug, PartialEq)]
pub(crate) struct MigrationV1;

#[async_trait]
impl Migrate for MigrationV1 {
    async fn migrate(self, logger: slog::Logger) -> MigrationResult<Option<Migration>> {
        info!(logger, "Performed migration v1");

        Ok(None)
    }
}

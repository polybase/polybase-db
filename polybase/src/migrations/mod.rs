mod migration_v1;

use crate::db;

use async_trait::async_trait;
use slog::{debug, info};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("invalid migration version")]
    InvalidMigrationVersionError,

    #[error("migration error - db error")]
    DbError(#[from] db::Error),
}

pub type MigrationResult<T> = std::result::Result<T, MigrationError>;

#[async_trait]
pub(crate) trait Migrate {
    async fn migrate(self, logger: slog::Logger) -> MigrationResult<Option<Migration>>;
}

macro_rules! impl_default_for {
        ($($name:ty),*) => {
            $(
                impl Default for $name {
                    fn default() -> Self {
                        Self
                    }
                }
            )*
        }
    }

impl_default_for!(migration_v1::MigrationV1);

// the migration types
#[derive(Debug, PartialEq)]
pub(crate) enum Migration {
    V1(migration_v1::MigrationV1),
}

#[async_trait]
impl Migrate for Migration {
    async fn migrate(self, logger: slog::Logger) -> MigrationResult<Option<Self>> {
        match self {
            Migration::V1(v1) => v1.migrate(logger.clone()).await,
        }
    }
}

// the migration orchestrator
pub struct Migrator {
    logger: slog::Logger,
    old_version: Option<u32>,
}

impl Migrator {
    pub fn new(logger: slog::Logger, old_version: Option<u32>) -> Self {
        Self {
            logger,
            old_version,
        }
    }

    pub async fn migrate(&self) -> MigrationResult<()> {
        let mut curr_stage = match self.old_version {
            None => Some(Migration::V1(migration_v1::MigrationV1::default())),
            Some(old_vers) => match old_vers {
                1 => None,
                _ => return Err(MigrationError::InvalidMigrationVersionError),
            },
        };

        while let Some(stage) = curr_stage {
            curr_stage = stage.migrate(self.logger.clone()).await?;
        }

        Ok(())
    }
}

/// Query the database for POLYBASE_DATABASE_VERSION, If the version is
/// not found or the version is less than the CURRENT_POLYBASE_VERSION, trigger
/// a migration.
pub async fn check_for_migrations(db: Arc<db::Db>, logger: slog::Logger) -> MigrationResult<()> {
    info!(logger, "[Migrations] Checking if migration is needed");

    match db.get_polybase_db_version().await? {
        Some(old_db_version) if old_db_version == db::CURRENT_POLYBASE_DB_VERSION => {
            info!(
                logger,
                "[Migrations] Polybase database version is up to date (v{}), no migrations needed",
                db::CURRENT_POLYBASE_DB_VERSION
            );
        }

        old_db_version => {
            if let Some(old_db_version) = old_db_version {
                info!(logger, "[Migrations] Old database version is {old_db_version}, current database version is: v{}", db::CURRENT_POLYBASE_DB_VERSION);
            } else {
                info!(
                    logger,
                    "[Migrations] Old database version not found, current database version is: v{}",
                    db::CURRENT_POLYBASE_DB_VERSION
                );
            }

            info!(
                logger,
                "[Migrations] Migration needed. Carrying out migrations"
            );

            // carry out the migrations
            let migrator = Migrator::new(logger.clone(), old_db_version);
            migrator.migrate().await?;

            // set the Polybase database version to the current version (in code)
            debug!(
                logger,
                "[Migrations] Setting Polybase database version to the current version (v{})",
                db::CURRENT_POLYBASE_DB_VERSION
            );
            db.set_polybase_db_version().await?;
            debug!(
                logger,
                "[Migrations] Finished setting Polybase database version to the current version"
            );

            info!(logger, "[Migrations] Migrations finished");
        }
    }

    Ok(())
}
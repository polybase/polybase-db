mod migration_v1;

use crate::db;

use async_trait::async_trait;
use slog::{crit, debug, info};
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
    async fn migrate(self) -> MigrationResult<Option<Migration>>;
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
    End,
}

#[async_trait]
impl Migrate for Migration {
    async fn migrate(self) -> MigrationResult<Option<Self>> {
        match self {
            Migration::V1(v1) => {
                println!("performed migration v1");
                Ok(Some(Migration::End))
            }

            Migration::End => Ok(None),
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
                1 => Some(Migration::End),
                _ => return Err(MigrationError::InvalidMigrationVersionError),
            },
        };

        while let Some(stage) = curr_stage {
            if stage == Migration::End {
                break;
            }
            curr_stage = stage.migrate().await?;
        }

        Ok(())
    }
}

/// Query the database for POLYBASE_DATABASE_VERSION, If the version is
/// not found or the version is less than the CURRENT_POLYBASE_VERSION, trigger
/// a migration.
pub async fn check_for_migrations(db: Arc<db::Db>, logger: slog::Logger) -> MigrationResult<()> {
    info!(logger, "[Migrations] checking if migration is needed");

    match db.get_polybase_db_version().await? {
        Some(old_db_version) if old_db_version == db::CURRENT_POLYBASE_DB_VERSION => {
            info!(
                logger,
                "[Migrations] Polybase database version is up to date, no migrations needed"
            );
        }

        old_db_version => {
            info!(
                logger,
                "[Migrations] migration needed. Carrying out migrations"
            );

            let migrator = Migrator::new(logger.clone(), old_db_version);
            migrator.migrate().await?;

            info!(logger, "[Migrations] migrations finished");
        }
    }

    Ok(())
}
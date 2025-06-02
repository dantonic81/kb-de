from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger("alembic")

# Load environment variables
load_dotenv()

# Get Alembic config object
config = context.config

# Configure logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def get_database_url(db_name=None):
    """Generate appropriate database URL based on environment and overrides"""
    env = os.getenv("ENV", "prod")
    db_host = "localhost" if env == "local" else "db"
    db_user = os.getenv("POSTGRES_USER", "user")
    db_pass = os.getenv("POSTGRES_PASSWORD", "pass")
    default_db = os.getenv("POSTGRES_DB", "health_data")
    target_db = db_name if db_name else default_db

    return f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{target_db}"


# Determine target database
TEST_DB = os.getenv("TEST_DB")
MIGRATION_DB = os.getenv("MIGRATION_DB")
database_url = get_database_url(TEST_DB or MIGRATION_DB)

# Inject the DB URL into Alembic's config
config.set_main_option("sqlalchemy.url", database_url)
logger.info(f"\n🔧 Applying migrations to database: {database_url}\n")

# Import all models for autogenerate support
from app.db.base import Base
from app.db.models import *

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
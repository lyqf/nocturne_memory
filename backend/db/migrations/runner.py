import os
import importlib.util
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

async def run_migrations(engine: AsyncEngine):
    """
    Run all pending migrations in order.
    Keeps track of applied migrations in the 'schema_migrations' table.
    """
    # Ensure migrations table exists
    async with engine.begin() as conn:
        await conn.execute(text(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "    version TEXT PRIMARY KEY, "
            "    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")"
        ))
        
    # Get applied migrations
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC"))
        applied_versions = {row[0] for row in result.fetchall()}

    # Discover migration files
    migrations_dir = os.path.dirname(__file__)
    migration_files = []
    for file in os.listdir(migrations_dir):
        if file.endswith(".py") and file[0].isdigit():
            migration_files.append(file)
            
    migration_files.sort()

    pending_migrations = [f for f in migration_files if f not in applied_versions]

    if not pending_migrations:
        return

    # Backup SQLite database before migration
    if str(engine.url).startswith("sqlite"):
        db_path = engine.url.database
        if db_path and db_path != ":memory:" and os.path.exists(db_path):
            import shutil
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{db_path}.{timestamp}.bak"
            logger.info(f"Pending migrations detected. Backing up SQLite database to {backup_path}")
            try:
                shutil.copy2(db_path, backup_path)
            except Exception as e:
                logger.error(f"Failed to backup database: {e}. Aborting migration.")
                raise RuntimeError(f"Database backup failed: {e}") from e

    for file in pending_migrations:
        logger.info(f"Applying migration: {file}")
        
        # Dynamically import the migration module
        safe_stem = file[:-3].replace(".", "_")
        module_name = f"backend.db.migrations.{safe_stem}"
        spec = importlib.util.spec_from_file_location(module_name, os.path.join(migrations_dir, file))
        module = importlib.util.module_from_spec(spec)
        module.__package__ = "backend.db.migrations"
        spec.loader.exec_module(module)
        
        # Execute the migration
        if hasattr(module, 'up'):
            await module.up(engine)
        else:
            logger.warning(f"Warning: {file} has no 'up' async function.")

        # Record the migration
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO schema_migrations (version) VALUES (:version)"),
                {"version": file}
            )

    logger.info("Successfully applied all pending migrations.")


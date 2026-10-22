import hashlib
import os
import re
import sys
from pathlib import Path

import psycopg


# Configuration
MIGRATIONS_DIR = Path(__file__).parent / "migrations"

ADVISORY_LOCK_ID = 728194620135

MIGRATION_PATTERN = re.compile(r"\d{4}_[a-z0-9_]+\.sql")


CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def discover_migrations():
    """Find and sort files that match the migration naming convention."""
    if not MIGRATIONS_DIR.is_dir():
        raise FileNotFoundError(
            f"Migrations directory does not exist: {MIGRATIONS_DIR}"
        )

    migrations = [
        path
        for path in MIGRATIONS_DIR.iterdir()
        if path.is_file() and MIGRATION_PATTERN.fullmatch(path.name)
    ]

    migrations.sort(key=lambda path: path.name)

    # Map each four-digit version number to its filename.
    seen_versions = {}

    for path in migrations:
        version_number = path.name[:4]

        if version_number in seen_versions:
            first_file = seen_versions[version_number]

            raise ValueError(
                f"Duplicate migration number {version_number}: "
                f"'{first_file}' and '{path.name}'"
            )

        seen_versions[version_number] = path.name

    return migrations


def read_migration(path):
    """Create the SHA-256 checksum of a migration file."""
    data = path.read_bytes()

    checksum = hashlib.sha256(data).hexdigest()
    sql = data.decode("utf-8")

    return checksum, sql


def verify_applied_migrations(conn, migrations):
    """Verify that applied migration files have not been modified."""
    rows = conn.execute(
        "SELECT version, checksum FROM schema_migrations"
    ).fetchall()

    applied = {version: checksum for version, checksum in rows}
    discovered = {path.name: path for path in migrations}

    # Detect missing historical files as well as edited files.
    missing = set(applied) - set(discovered)

    if missing:
        names = ", ".join(sorted(missing))
        raise RuntimeError(
            f"Applied migration files are missing from disk: {names}"
        )

    for version, stored_checksum in applied.items():
        path = discovered[version]
        current_checksum, _ = read_migration(path)

        if current_checksum != stored_checksum:
            raise RuntimeError(
                f"Checksum mismatch for {version}. "
                "An already-applied migration was modified. "
                "Restore the original file or create a new migration."
            )

        print(f"VERIFIED {version}", flush=True)


def apply_migration(conn, path):
    """Apply one migration and record it atomically."""
    version = path.name
    checksum, sql = read_migration(path)

    try:

        # The migration SQL and its tracking row share one transaction.
        with conn.transaction():
            conn.execute(sql)

            conn.execute(
                """
                INSERT INTO schema_migrations (version, checksum)
                VALUES (%s, %s)
                """,
                (version, checksum),
            )

        print(f"APPLIED  {version}", flush=True)

    except Exception as exc:
        raise RuntimeError(f"{version}: {exc}", file=sys.stderr)


def run_migrations():
    dsn = os.environ.get("DATABASE_URL")

    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    migrations = discover_migrations()

    # Autocommit allows bootstrap and lock acquisition to happen
    # independently of the per-migration transactions.
    with psycopg.connect(dsn, autocommit=True) as conn:

        # 1. Serialize migration runners using a session-level lock.
        conn.execute(
            "SELECT pg_advisory_lock(%s)",
            (ADVISORY_LOCK_ID,),
        )

        print("Acquired migration lock.", flush=True)

        # 2. Bootstrap: safe to execute on every startup.
        conn.execute(CREATE_TRACKING_TABLE)

        # 3–4. Verify all previously applied migration files.
        verify_applied_migrations(conn, migrations)

        # 5. Apply pending migrations in filename order.
        applied_rows = conn.execute(
            "SELECT version FROM schema_migrations"
        ).fetchall()

        applied_versions = {row[0] for row in applied_rows}

        for path in migrations:
            if path.name in applied_versions:
                continue

            apply_migration(conn, path)

        print("All migrations are up to date.", flush=True)


def main():
    try:
        run_migrations()
        return 0

    except Exception as exc:
        print(f"Migration runner failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

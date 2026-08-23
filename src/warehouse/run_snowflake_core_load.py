from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import snowflake.connector


DEFAULT_MANIFEST = Path("configs/warehouse/snowflake_core_load_manifest.json")

IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
STAGE_PATH_PATTERN = re.compile(r"^[A-Za-z0-9_./=\-]+$")


class CoreLoadError(RuntimeError):
    """Raised when Snowflake CORE ingestion fails."""


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise CoreLoadError(f"Required environment variable is not set: {name}")
    return value


def validate_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise CoreLoadError(f"Invalid {label}: {value!r}")
    return value


def validate_stage_path(value: str) -> str:
    if not value or not STAGE_PATH_PATTERN.fullmatch(value):
        raise CoreLoadError(f"Invalid stage_path: {value!r}")

    if ".." in value:
        raise CoreLoadError(f"stage_path must not contain '..': {value!r}")

    return value.strip("/") + "/"


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise CoreLoadError(f"Manifest not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    if manifest.get("manifest_version") != 1:
        raise CoreLoadError(
            f"Unsupported manifest_version: {manifest.get('manifest_version')!r}"
        )

    target = manifest.get("target")
    tables = manifest.get("tables")

    if not isinstance(target, dict):
        raise CoreLoadError("Manifest target must be an object.")

    if not isinstance(tables, list) or not tables:
        raise CoreLoadError("Manifest tables must be a non-empty list.")

    required_target_fields = {
        "database",
        "schema",
        "stage",
        "file_format",
        "file_pattern",
    }

    missing = required_target_fields - target.keys()
    if missing:
        raise CoreLoadError(f"Manifest target missing fields: {sorted(missing)}")

    validate_identifier(target["database"], "database")
    validate_identifier(target["schema"], "schema")
    validate_identifier(target["stage"], "stage")
    validate_identifier(target["file_format"], "file_format")

    seen_tables: set[str] = set()

    for entry in tables:
        for field in (
            "table",
            "source_product",
            "stage_path",
            "expected_rows",
            "expected_columns",
            "primary_key",
        ):
            if field not in entry:
                raise CoreLoadError(
                    f"Manifest table entry missing {field}: {entry}"
                )

        table = validate_identifier(entry["table"], "table")
        validate_identifier(entry["primary_key"], "primary_key")
        validate_stage_path(entry["stage_path"])

        if table in seen_tables:
            raise CoreLoadError(f"Duplicate table in manifest: {table}")

        seen_tables.add(table)

        if entry["expected_rows"] < 0:
            raise CoreLoadError(f"{table}: expected_rows must be >= 0")

        if entry["expected_columns"] <= 0:
            raise CoreLoadError(f"{table}: expected_columns must be > 0")

    return manifest


def selected_tables(
    manifest: dict[str, Any],
    requested: list[str] | None,
) -> list[dict[str, Any]]:
    tables = manifest["tables"]

    if not requested:
        return tables

    requested_upper = [name.upper() for name in requested]
    available = {entry["table"]: entry for entry in tables}

    missing = [name for name in requested_upper if name not in available]
    if missing:
        raise CoreLoadError(
            "Requested table(s) not found in manifest: " + ", ".join(missing)
        )

    return [available[name] for name in requested_upper]


def fq_name(database: str, schema: str, object_name: str) -> str:
    validate_identifier(database, "database")
    validate_identifier(schema, "schema")
    validate_identifier(object_name, "object")

    return f'"{database}"."{schema}"."{object_name}"'


def create_connection(database: str, schema: str):
    return snowflake.connector.connect(
        account=require_env("SNOWFLAKE_ACCOUNT"),
        user=require_env("SNOWFLAKE_USER"),
        password=require_env("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=require_env("SNOWFLAKE_WAREHOUSE"),
        database=database,
        schema=schema,
        autocommit=True,
    )


def ensure_audit_table(cursor, database: str) -> None:
    audit_table = fq_name(database, "AUDIT", "CORE_LOAD_RUN")

    cursor.execute(
        f"""
        create table if not exists {audit_table} (
            load_run_id varchar not null,
            table_name varchar not null,
            source_product varchar not null,
            stage_path varchar not null,
            started_at timestamp_tz not null,
            finished_at timestamp_tz,
            status varchar not null,
            expected_rows number,
            actual_rows number,
            expected_columns number,
            actual_columns number,
            primary_key varchar,
            distinct_key_count number,
            null_key_count number,
            promoted boolean,
            error_message varchar
        )
        """
    )


def write_audit(
    cursor,
    *,
    database: str,
    load_run_id: str,
    entry: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
    status: str,
    actual_rows: int | None,
    actual_columns: int | None,
    distinct_key_count: int | None,
    null_key_count: int | None,
    promoted: bool,
    error_message: str | None,
) -> None:
    audit_table = fq_name(database, "AUDIT", "CORE_LOAD_RUN")

    cursor.execute(
        f"""
        insert into {audit_table} (
            load_run_id,
            table_name,
            source_product,
            stage_path,
            started_at,
            finished_at,
            status,
            expected_rows,
            actual_rows,
            expected_columns,
            actual_columns,
            primary_key,
            distinct_key_count,
            null_key_count,
            promoted,
            error_message
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            load_run_id,
            entry["table"],
            entry["source_product"],
            entry["stage_path"],
            started_at,
            finished_at,
            status,
            entry["expected_rows"],
            actual_rows,
            entry["expected_columns"],
            actual_columns,
            entry["primary_key"],
            distinct_key_count,
            null_key_count,
            promoted,
            error_message,
        ),
    )


def table_exists(
    cursor,
    *,
    database: str,
    schema: str,
    table: str,
) -> bool:
    cursor.execute(
        f"""
        select count(*)
        from "{database}".information_schema.tables
        where table_schema = %s
          and table_name = %s
        """,
        (schema, table),
    )

    return cursor.fetchone()[0] == 1

def latest_promoted_stage_path(
    cursor,
    *,
    database: str,
    table: str
) -> str | None:
    audit_table = fq_name(database, "AUDIT", "CORE_LOAD_RUN")
    cursor.execute(
        f"""
        select stage_path
        from {audit_table}
        where table_name = %s
            and status in ('SUCCESS', 'ADOPTED')
            and promoted = true
        order by finished_at desc
        limit 1
        """,
        (table,),
    )
    row = cursor.fetchone()
    return None if row is None else row[0]


def get_column_count(
    cursor,
    *,
    database: str,
    schema: str,
    table: str,
) -> int:
    cursor.execute(
        f"""
        select count(*)
        from "{database}".information_schema.columns
        where table_schema = %s
          and table_name = %s
        """,
        (schema, table),
    )

    return int(cursor.fetchone()[0])


def validate_candidate(
    cursor,
    *,
    candidate_fqn: str,
    primary_key: str,
    expected_rows: int,
    expected_columns: int,
    database: str,
    schema: str,
    candidate_name: str,
) -> tuple[int, int, int, int]:
    cursor.execute(
        f"""
        select
            count(*) as row_count,
            count(distinct "{primary_key}") as distinct_key_count,
            count_if("{primary_key}" is null) as null_key_count
        from {candidate_fqn}
        """
    )

    row = cursor.fetchone()

    actual_rows = int(row[0])
    distinct_key_count = int(row[1])
    null_key_count = int(row[2])

    actual_columns = get_column_count(
        cursor,
        database=database,
        schema=schema,
        table=candidate_name,
    )

    errors: list[str] = []

    if actual_rows != expected_rows:
        errors.append(
            f"row count mismatch: expected={expected_rows}, actual={actual_rows}"
        )

    if actual_columns != expected_columns:
        errors.append(
            f"column count mismatch: expected={expected_columns}, actual={actual_columns}"
        )

    if distinct_key_count != actual_rows:
        errors.append(
            f"primary key uniqueness failed: rows={actual_rows}, "
            f"distinct_keys={distinct_key_count}"
        )

    if null_key_count != 0:
        errors.append(f"primary key null check failed: null_keys={null_key_count}")

    if errors:
        raise CoreLoadError("; ".join(errors))

    return (
        actual_rows,
        actual_columns,
        distinct_key_count,
        null_key_count,
    )

def adopt_existing_table(
    connection,
    *,
    target: dict[str, Any],
    entry: dict[str, Any],
) -> None:
    database = target["database"]
    schema = target["schema"]

    table = entry["table"]
    primary_key = entry["primary_key"]
    stage_path = validate_stage_path(entry["stage_path"])

    load_run_id = str(uuid.uuid4())
    target_fqn = fq_name(database, schema, table)

    started_at = datetime.now(timezone.utc)

    actual_rows = None
    actual_columns = None
    distinct_key_count = None
    null_key_count = None

    print()
    print(f"[ADOPT] {table}")
    print(f"  source : {entry['source_product']}")
    print(f"  path   : {stage_path}")
    print(f"  target : {target_fqn}")

    cursor = connection.cursor()

    try:
        if not table_exists(
            cursor,
            database=database,
            schema=schema,
            table=table,
        ):
            raise CoreLoadError(
                f"Cannot adopt {table}: production table does not exist."
            )

        current_stage_path = latest_promoted_stage_path(
            cursor,
            database=database,
            table=table,
        )

        if current_stage_path == stage_path:
            print(
                f"[SKIP]  {table} "
                f"(snapshot already registered)"
            )
            return

        (
            actual_rows,
            actual_columns,
            distinct_key_count,
            null_key_count,
        ) = validate_candidate(
            cursor,
            candidate_fqn=target_fqn,
            primary_key=primary_key,
            expected_rows=entry["expected_rows"],
            expected_columns=entry["expected_columns"],
            database=database,
            schema=schema,
            candidate_name=table,
        )

        finished_at = datetime.now(timezone.utc)

        write_audit(
            cursor,
            database=database,
            load_run_id=load_run_id,
            entry=entry,
            started_at=started_at,
            finished_at=finished_at,
            status="ADOPTED",
            actual_rows=actual_rows,
            actual_columns=actual_columns,
            distinct_key_count=distinct_key_count,
            null_key_count=null_key_count,
            promoted=True,
            error_message=None,
        )

        print(
            f"  validation: rows={actual_rows:,}, "
            f"columns={actual_columns}, "
            f"distinct_pk={distinct_key_count:,}, "
            f"null_pk={null_key_count}"
        )
        print(f"[ADOPTED] {table}")

    except Exception as exc:
        finished_at = datetime.now(timezone.utc)

        try:
            write_audit(
                cursor,
                database=database,
                load_run_id=load_run_id,
                entry=entry,
                started_at=started_at,
                finished_at=finished_at,
                status="FAILED",
                actual_rows=actual_rows,
                actual_columns=actual_columns,
                distinct_key_count=distinct_key_count,
                null_key_count=null_key_count,
                promoted=False,
                error_message=str(exc)[:16000],
            )
        except Exception:
            pass

        raise CoreLoadError(f"{table}: {exc}") from exc

    finally:
        cursor.close()


def load_one_table(
    connection,
    *,
    target: dict[str, Any],
    entry: dict[str, Any],
) -> None:
    database = target["database"]
    schema = target["schema"]
    stage = target["stage"]
    file_format = target["file_format"]
    file_pattern = target["file_pattern"]

    table = entry["table"]
    primary_key = entry["primary_key"]
    stage_path = validate_stage_path(entry["stage_path"])

    load_run_id = str(uuid.uuid4())
    suffix = load_run_id.replace("-", "")[:8].upper()

    candidate_name = f"LOAD_{table}_{suffix}"

    target_fqn = fq_name(database, schema, table)
    candidate_fqn = fq_name(database, schema, candidate_name)

    stage_fqn = fq_name(database, schema, stage)
    file_format_fqn = fq_name(database, schema, file_format)

    stage_location = f"@{stage_fqn}/{stage_path}"

    started_at = datetime.now(timezone.utc)

    actual_rows = None
    actual_columns = None
    distinct_key_count = None
    null_key_count = None
    promoted = False

    print()
    print(f"[START] {table}")
    print(f"  source : {entry['source_product']}")
    print(f"  path   : {stage_path}")
    print(f"  target : {target_fqn}")
    print(f"  temp   : {candidate_fqn}")

    cursor = connection.cursor()

    try:
        current_stage_path = latest_promoted_stage_path(
            cursor,
            database=database,
            table=table,
        )
        
        production_exists = table_exists(
            cursor,
            database=database,
            schema=schema,
            table=table,
        )
        
        if production_exists and current_stage_path == stage_path:
            print(
                f"[SKIP] {table} "
                f"(snapshot already promoted)"
            )
            return
        
        # 1. Build an isolated candidate table from Parquet schema.
        cursor.execute(
            f"""
            create or replace table {candidate_fqn}
            using template (
                select array_agg(object_construct(*))
                from table(
                    infer_schema(
                        location => '{stage_location}',
                        file_format => '{file_format_fqn}',
                        ignore_case => true
                    )
                )
            )
            """
        )

        # 2. Load only the frozen Gold snapshot from the manifest.
        cursor.execute(
            f"""
            copy into {candidate_fqn}
            from {stage_location}
            file_format = (
                format_name = '{file_format_fqn}'
            )
            match_by_column_name = case_insensitive
            pattern = '{file_pattern}'
            on_error = 'ABORT_STATEMENT'
            """
        )

        # 3. Validate before touching the production CORE table.
        (
            actual_rows,
            actual_columns,
            distinct_key_count,
            null_key_count,
        ) = validate_candidate(
            cursor,
            candidate_fqn=candidate_fqn,
            primary_key=primary_key,
            expected_rows=entry["expected_rows"],
            expected_columns=entry["expected_columns"],
            database=database,
            schema=schema,
            candidate_name=candidate_name,
        )

        print(
            f"  validation: rows={actual_rows:,}, "
            f"columns={actual_columns}, "
            f"distinct_pk={distinct_key_count:,}, "
            f"null_pk={null_key_count}"
        )

        # 4. Promote only after all validation passes.
        if table_exists(
            cursor,
            database=database,
            schema=schema,
            table=table,
        ):
            cursor.execute(f"alter table {target_fqn} swap with {candidate_fqn}")

            # After SWAP, candidate now points to the old production table.
            cursor.execute(f"drop table {candidate_fqn}")

        else:
            cursor.execute(f"alter table {candidate_fqn} rename to {target_fqn}")

        promoted = True
        finished_at = datetime.now(timezone.utc)

        write_audit(
            cursor,
            database=database,
            load_run_id=load_run_id,
            entry=entry,
            started_at=started_at,
            finished_at=finished_at,
            status="SUCCESS",
            actual_rows=actual_rows,
            actual_columns=actual_columns,
            distinct_key_count=distinct_key_count,
            null_key_count=null_key_count,
            promoted=True,
            error_message=None,
        )

        print(f"[PASS]  {table}")

    except Exception as exc:
        finished_at = datetime.now(timezone.utc)

        # Candidate is disposable. The existing CORE table is untouched
        # unless promotion already completed successfully.
        try:
            if not promoted and table_exists(
                cursor,
                database=database,
                schema=schema,
                table=candidate_name,
            ):
                cursor.execute(f"drop table {candidate_fqn}")
        except Exception:
            pass

        try:
            write_audit(
                cursor,
                database=database,
                load_run_id=load_run_id,
                entry=entry,
                started_at=started_at,
                finished_at=finished_at,
                status="FAILED",
                actual_rows=actual_rows,
                actual_columns=actual_columns,
                distinct_key_count=distinct_key_count,
                null_key_count=null_key_count,
                promoted=promoted,
                error_message=str(exc)[:16000],
            )
        except Exception:
            pass

        raise CoreLoadError(f"{table}: {exc}") from exc

    finally:
        cursor.close()


def print_plan(
    *,
    manifest_path: Path,
    target: dict[str, Any],
    tables: list[dict[str, Any]],
) -> None:
    print("Snowflake CORE load plan")
    print(f"manifest : {manifest_path}")
    print(f"target   : {target['database']}.{target['schema']}")
    print(f"stage    : {target['database']}.{target['schema']}.{target['stage']}")
    print(f"tables   : {len(tables)}")
    print()

    for entry in tables:
        print(
            f"- {entry['table']}: "
            f"{entry['expected_rows']:,} rows, "
            f"{entry['expected_columns']} columns"
        )
        print(f"  {entry['stage_path']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load frozen canonical ADLS Gold snapshots into Snowflake CORE."
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help=f"Load manifest. Default: {DEFAULT_MANIFEST}",
    )

    parser.add_argument(
        "--table",
        action="append",
        help="Load only this CORE table. May be supplied multiple times.",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Execute Snowflake loads. "
            "Without this flag the command is dry-run only."
        ),
    )
    
    parser.add_argument(
        "--adopt-existing",
        action="store_true",
        help=(
            "Validation existing CORE tables against the manifest "
            "and register them as the active frozen snapshot "
            "without reloading data. "
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        manifest = load_manifest(args.manifest)

        target = manifest["target"]

        tables = selected_tables(manifest, args.table)

        print_plan(
            manifest_path=args.manifest,
            target=target,
            tables=tables,
        )

        if not args.execute:
            print()
            print("DRY RUN ONLY. No Snowflake changes were made.")
            return 0

        connection = create_connection(
            database=target["database"],
            schema=target["schema"],
        )

        try:
            cursor = connection.cursor()

            try:
                ensure_audit_table(cursor, target["database"])
            finally:
                cursor.close()

            for entry in tables:
                if args.adopt_existing:
                    adopt_existing_table(
                        connection,
                        target=target,
                        entry=entry,
                    )
                else:
                    load_one_table(connection, target=target, entry=entry)

        finally:
            connection.close()

        print()
        print(f"CORE load complete: {len(tables)} table(s) succeeded.")

        return 0

    except CoreLoadError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
import mysql.connector
import psycopg2
import csv
import logging
import os
from datetime import datetime
from psycopg2 import sql
from io import StringIO
import re

# pip install mysql-connector-python psycopg2-binary

# ================= CONFIG =================

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "cyber_users"
}

PG_CONFIG = {
    "host": "localhost",
    "user": "cyber_user",
    "password": "cyber_user",
    "dbname": "cyber_users",
    "schema": "cyber"
}

# ==========================================

# Always write the log file next to this script, regardless of where
# you run it from (e.g. python scripts/migrate.py from project root)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

# Each run gets its own timestamped file  →  logs/migration_2026-03-19_14-30-00.log
_LOG_FILE = os.path.join(
    _LOG_DIR,
    f"migration_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
)

logging.basicConfig(
    level=logging.DEBUG,  # DEBUG so nothing is silently dropped
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),  # console  (INFO and above)
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),  # file (everything)
    ]
)

# Tone down console to INFO only — file still gets DEBUG
logging.getLogger().handlers[0].setLevel(logging.INFO)

log = logging.getLogger(__name__)
log.info(f"Log file: {_LOG_FILE}")


# -------------------------------------------------------------------
# tinyint(1)  →  BOOLEAN  (MySQL convention for booleans)
# All other tinyint widths → INTEGER
# -------------------------------------------------------------------
def map_type(mysql_type: str) -> str:
    lower = mysql_type.lower()
    if lower == "tinyint(1)":
        return "BOOLEAN"
    base = lower.split("(")[0]
    mapping = {
        "int": "INTEGER",
        "bigint": "BIGINT",
        "varchar": "VARCHAR",
        "text": "TEXT",
        "datetime": "TIMESTAMP",
        "double": "DOUBLE PRECISION",
        "tinyint": "INTEGER",
        "date": "DATE",
        "float": "DOUBLE PRECISION",
    }
    return mapping.get(base, "TEXT")


def to_snake_case(name: str) -> str:
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    name = name.replace("-", "_")
    name = re.sub(r'_+', '_', name)  # collapse __ → _
    name = name.strip('_')  # remove leading / trailing _
    return name.lower()


# -------------------------------------------------------------------
# Helpers: open / close connections
# -------------------------------------------------------------------
def get_mysql_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_CONFIG["host"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        dbname=PG_CONFIG["dbname"]
    )


# -------------------------------------------------------------------
# 1. Schema migration
# -------------------------------------------------------------------
def migrate_schema():
    log.info("=== Migrating Schema ===")
    schema = PG_CONFIG["schema"]

    mysql_conn = get_mysql_conn()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    pg_cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    mysql_cur.execute("SHOW TABLES")
    tables = [list(row.values())[0] for row in mysql_cur.fetchall()]

    for table in tables:
        pg_table = to_snake_case(table)
        log.info(f"  Creating table: {schema}.{pg_table}")

        # Full column info including the raw type string (for tinyint(1) detection)
        mysql_cur.execute("""
            SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE,
                   COLUMN_KEY, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (MYSQL_CONFIG["database"], table))

        columns = mysql_cur.fetchall()
        column_defs = []

        for col in columns:
            pg_col = to_snake_case(col["COLUMN_NAME"])
            pg_type = map_type(col["COLUMN_TYPE"])  # use full type e.g. tinyint(1)
            extra = col["EXTRA"] or ""
            nullable = "" if col["IS_NULLABLE"] == "YES" else "NOT NULL"

            if "auto_increment" in extra:
                column_defs.append(f'"{pg_col}" SERIAL PRIMARY KEY')
                continue

            column_defs.append(f'"{pg_col}" {pg_type} {nullable}'.strip())

        create_sql = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{pg_table}" '
            f'({", ".join(column_defs)});'
        )
        pg_cur.execute(create_sql)

    pg_conn.commit()
    for c in (mysql_cur, pg_cur): c.close()
    for c in (mysql_conn, pg_conn): c.close()
    log.info("Schema migration complete.\n")


# -------------------------------------------------------------------
# 2. Data migration  (COPY with per-table try/except fallback)
# -------------------------------------------------------------------
def migrate_data2():
    log.info("=== Migrating Data ===")
    schema = PG_CONFIG["schema"]

    mysql_conn = get_mysql_conn()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("SHOW TABLES")
    tables = [list(row.values())[0] for row in mysql_cur.fetchall()]

    for table in tables:
        pg_table = to_snake_case(table)
        log.info(f"  Migrating data: {pg_table}")

        mysql_cur.execute("""
            SELECT COLUMN_NAME, COLUMN_TYPE, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (MYSQL_CONFIG["database"], table))
        cols_meta = mysql_cur.fetchall()

        # Skip auto_increment (SERIAL handles those)
        mysql_columns = [
            c["COLUMN_NAME"] for c in cols_meta
            if "auto_increment" not in (c["EXTRA"] or "")
        ]
        if not mysql_columns:
            continue

        select_sql = "SELECT {} FROM `{}`".format(
            ", ".join(f"`{c}`" for c in mysql_columns), table
        )
        mysql_cur.execute(select_sql)
        rows = mysql_cur.fetchall()
        if not rows:
            log.info(f"    (no rows, skipping)")
            continue

        pg_columns = [to_snake_case(c) for c in mysql_columns]
        col_list = ", ".join(f'"{c}"' for c in pg_columns)

        # --- build CSV buffer ---
        buf = StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow([row[c] for c in mysql_columns])
        buf.seek(0)

        copy_sql = (
            f'COPY "{schema}"."{pg_table}" ({col_list}) '
            f"FROM STDIN WITH CSV NULL ''"
        )

        # Primary path: fast COPY
        try:
            pg_cur.copy_expert(copy_sql, buf)
            pg_conn.commit()

        except Exception as copy_err:
            pg_conn.rollback()
            log.warning(
                f"    COPY failed for {pg_table} ({copy_err}). "
                f"Falling back to row-by-row insert…"
            )

            # Fallback: row-by-row INSERT — isolates the bad row
            placeholders = ", ".join(["%s"] * len(pg_columns))
            insert_sql = (
                f'INSERT INTO "{schema}"."{pg_table}" ({col_list}) '
                f"VALUES ({placeholders})"
            )
            ok = fail = 0
            for row in rows:
                values = [row[c] for c in mysql_columns]
                try:
                    pg_cur.execute(insert_sql, values)
                    pg_conn.commit()
                    ok += 1
                except Exception as row_err:
                    pg_conn.rollback()
                    log.error(f"      Skipped row in {pg_table}: {row_err} | data={values}")
                    fail += 1

            log.info(f"    Fallback done — inserted: {ok}, skipped: {fail}")

    for c in (mysql_cur, pg_cur): c.close()
    for c in (mysql_conn, pg_conn): c.close()
    log.info("Data migration complete.\n")


def migrate_data():
    log.info("=== Migrating Data ===")
    schema = PG_CONFIG["schema"]

    mysql_conn = get_mysql_conn()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("SHOW TABLES")
    tables = [list(row.values())[0] for row in mysql_cur.fetchall()]

    for table in tables:
        pg_table = to_snake_case(table)
        log.info(f"  Migrating data: {pg_table}")

        mysql_cur.execute("""
            SELECT COLUMN_NAME, COLUMN_TYPE, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (MYSQL_CONFIG["database"], table))
        cols_meta = mysql_cur.fetchall()

        # ✅ FIX: Include ALL columns, including auto_increment (PK)
        mysql_columns = [c["COLUMN_NAME"] for c in cols_meta]

        # Find the PK column name (if any) for sequence handling
        pk_col_mysql = next(
            (c["COLUMN_NAME"] for c in cols_meta if "auto_increment" in (c["EXTRA"] or "")),
            None
        )
        pk_col_pg = to_snake_case(pk_col_mysql) if pk_col_mysql else None

        if not mysql_columns:
            continue

        select_sql = "SELECT {} FROM `{}`".format(
            ", ".join(f"`{c}`" for c in mysql_columns), table
        )
        mysql_cur.execute(select_sql)
        rows = mysql_cur.fetchall()
        if not rows:
            log.info(f"    (no rows, skipping)")
            continue

        pg_columns = [to_snake_case(c) for c in mysql_columns]
        col_list = ", ".join(f'"{c}"' for c in pg_columns)

        # ✅ FIX: Drop the SERIAL default so COPY can insert explicit PK values
        if pk_col_pg:
            seq_name = f"{schema}.{pg_table}_{pk_col_pg}_seq"
            try:
                pg_cur.execute(
                    f'ALTER TABLE "{schema}"."{pg_table}" '
                    f'ALTER COLUMN "{pk_col_pg}" DROP DEFAULT;'
                )
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                log.warning(f"  Could not drop default for {pg_table}.{pk_col_pg}: {e}")

        # --- build CSV buffer ---
        buf = StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow([row[c] for c in mysql_columns])
        buf.seek(0)

        copy_sql = (
            f'COPY "{schema}"."{pg_table}" ({col_list}) '
            f"FROM STDIN WITH CSV NULL ''"
        )

        # Primary path: fast COPY
        try:
            pg_cur.copy_expert(copy_sql, buf)
            pg_conn.commit()

        except Exception as copy_err:
            pg_conn.rollback()
            log.warning(
                f"    COPY failed for {pg_table} ({copy_err}). "
                f"Falling back to row-by-row insert…"
            )

            # ✅ FIX: Use OVERRIDING SYSTEM VALUE to force explicit PK values
            placeholders = ", ".join(["%s"] * len(pg_columns))
            insert_sql = (
                f'INSERT INTO "{schema}"."{pg_table}" ({col_list}) '
                f"OVERRIDING SYSTEM VALUE "
                f"VALUES ({placeholders})"
            )
            ok = fail = 0
            for row in rows:
                values = [row[c] for c in mysql_columns]
                try:
                    pg_cur.execute(insert_sql, values)
                    pg_conn.commit()
                    ok += 1
                except Exception as row_err:
                    pg_conn.rollback()
                    log.error(f"      Skipped row in {pg_table}: {row_err} | data={values}")
                    fail += 1

            log.info(f"    Fallback done — inserted: {ok}, skipped: {fail}")

        # ✅ FIX: Restore the SERIAL default after COPY
        if pk_col_pg:
            seq_name = f'"{schema}"."{pg_table}_{pk_col_pg}_seq"'
            try:
                pg_cur.execute(
                    f'ALTER TABLE "{schema}"."{pg_table}" '
                    f'ALTER COLUMN "{pk_col_pg}" '
                    f'SET DEFAULT nextval({seq_name!r});'
                )
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                log.warning(f"  Could not restore default for {pg_table}.{pk_col_pg}: {e}")

    for c in (mysql_cur, pg_cur): c.close()
    for c in (mysql_conn, pg_conn): c.close()
    log.info("Data migration complete.\n")


# -------------------------------------------------------------------
# 3. Reset sequences so next INSERT doesn't conflict
# -------------------------------------------------------------------
def reset_sequences():
    log.info("=== Resetting PG Sequences ===")
    schema = PG_CONFIG["schema"]

    mysql_conn = get_mysql_conn()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("SHOW TABLES")
    tables = [list(row.values())[0] for row in mysql_cur.fetchall()]

    for table in tables:
        pg_table = to_snake_case(table)

        # Find the SERIAL column (auto_increment source)
        mysql_cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              AND EXTRA LIKE '%auto_increment%'
            LIMIT 1
        """, (MYSQL_CONFIG["database"], table))
        row = mysql_cur.fetchone()
        if not row:
            continue

        pg_col = to_snake_case(row["COLUMN_NAME"])

        try:
            pg_cur.execute(f"""
                SELECT setval(
                    pg_get_serial_sequence('"{schema}"."{pg_table}"', '{pg_col}'),
                    COALESCE(MAX("{pg_col}"), 1)
                )
                FROM "{schema}"."{pg_table}";
            """)
            pg_conn.commit()
            log.info(f"  Sequence reset: {schema}.{pg_table}.{pg_col}")
        except Exception as e:
            pg_conn.rollback()
            log.warning(f"  Could not reset sequence for {pg_table}: {e}")

    for c in (mysql_cur, pg_cur): c.close()
    for c in (mysql_conn, pg_conn): c.close()
    log.info("Sequence reset complete.\n")


# -------------------------------------------------------------------
# 4. Foreign key migration
# -------------------------------------------------------------------
def migrate_foreign_keys():
    log.info("=== Migrating Foreign Keys ===")
    schema = PG_CONFIG["schema"]

    mysql_conn = get_mysql_conn()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT
            kcu.TABLE_NAME,
            kcu.COLUMN_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
          ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
         AND kcu.TABLE_SCHEMA    = rc.CONSTRAINT_SCHEMA
        WHERE kcu.TABLE_SCHEMA = %s
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
    """, (MYSQL_CONFIG["database"],))

    fks = mysql_cur.fetchall()
    ok = fail = 0

    for fk in fks:
        child_table = to_snake_case(fk["TABLE_NAME"])
        child_col = to_snake_case(fk["COLUMN_NAME"])
        parent_table = to_snake_case(fk["REFERENCED_TABLE_NAME"])
        parent_col = to_snake_case(fk["REFERENCED_COLUMN_NAME"])
        constraint = to_snake_case(fk["CONSTRAINT_NAME"])

        alter_sql = (
            f'ALTER TABLE "{schema}"."{child_table}" '
            f'ADD CONSTRAINT "{constraint}" '
            f'FOREIGN KEY ("{child_col}") '
            f'REFERENCES "{schema}"."{parent_table}" ("{parent_col}");'
        )
        try:
            pg_cur.execute(alter_sql)
            pg_conn.commit()
            log.info(f"  FK added: {child_table}.{child_col} → {parent_table}.{parent_col}")
            ok += 1
        except Exception as e:
            pg_conn.rollback()
            log.warning(f"  FK skipped ({child_table}.{child_col}): {e}")
            fail += 1

    for c in (mysql_cur, pg_cur): c.close()
    for c in (mysql_conn, pg_conn): c.close()
    log.info(f"FK migration complete — added: {ok}, skipped: {fail}\n")


# -------------------------------------------------------------------
# 5. Verify row counts
# -------------------------------------------------------------------
def verify_counts():
    log.info("=== Verifying Row Counts ===")
    schema = PG_CONFIG["schema"]

    mysql_conn = get_mysql_conn()
    mysql_cur = mysql_conn.cursor()
    pg_conn = get_pg_conn()
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("SHOW TABLES")
    tables = [row[0] for row in mysql_cur.fetchall()]
    ok = fail = 0

    for table in tables:
        pg_table = to_snake_case(table)

        mysql_cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        mysql_count = mysql_cur.fetchone()[0]

        pg_cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{pg_table}"')
        pg_count = pg_cur.fetchone()[0]

        if mysql_count == pg_count:
            log.info(f"  ✅ {pg_table}: {mysql_count}")
            ok += 1
        else:
            log.warning(f"  ❌ {pg_table}: MySQL={mysql_count}, PG={pg_count}")
            fail += 1

    log.info(f"\nSummary → Matched: {ok} | Mismatched: {fail}")

    for c in (mysql_cur, pg_cur): c.close()
    for c in (mysql_conn, pg_conn): c.close()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    migrate_schema()
    migrate_data()
    reset_sequences()
    migrate_foreign_keys()
    verify_counts()

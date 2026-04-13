import mysql.connector
import psycopg2
import csv
from psycopg2 import sql
from io import StringIO


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
    "user": "postgres",
    "password": "postgres",
    "dbname": "cyber_users",
    "schema": "cyber"
}

# ==========================================

TYPE_MAPPING = {
    "int": "INTEGER",
    "bigint": "BIGINT",
    "varchar": "VARCHAR",
    "text": "TEXT",
    "datetime": "TIMESTAMP",
    "double": "DOUBLE PRECISION",
    "tinyint": "INTEGER",
    "date": "DATE",
    "float": "DOUBLE PRECISION"
}



def map_type(mysql_type):
    base = mysql_type.split("(")[0].lower()
    return TYPE_MAPPING.get(base, "TEXT")


def migrate_schema():
    print("\n=== Migrating Schema ===")

    schema_name = PG_CONFIG["schema"]

    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    pg_conn = psycopg2.connect(
        host=PG_CONFIG["host"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        dbname=PG_CONFIG["dbname"]
    )
    pg_cursor = pg_conn.cursor()

    # Create schema if not exists
    pg_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')

    mysql_cursor.execute("SHOW TABLES")
    tables = [list(row.values())[0] for row in mysql_cursor.fetchall()]

    for table in tables:
        print(f"Creating table: {table}")

        mysql_cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (MYSQL_CONFIG["database"], table))

        columns = mysql_cursor.fetchall()
        column_defs = []

        for col in columns:
            col_name = col["COLUMN_NAME"]
            data_type = map_type(col["DATA_TYPE"])
            extra = col["EXTRA"]

            if "auto_increment" in extra:
                column_defs.append(f'"{col_name}" SERIAL PRIMARY KEY')
                continue

            nullable = "NOT NULL" if col["IS_NULLABLE"] == "NO" else ""
            column_defs.append(f'"{col_name}" {data_type} {nullable}')

        create_query = f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table}"
            ({", ".join(column_defs)});
        '''

        pg_cursor.execute(create_query)

    pg_conn.commit()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()

    print("Schema migration complete.")


def migrate_data():
    print("\n=== Migrating Data ===")

    schema_name = PG_CONFIG["schema"]

    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()

    pg_conn = psycopg2.connect(
        host=PG_CONFIG["host"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        dbname=PG_CONFIG["dbname"]
    )
    pg_cursor = pg_conn.cursor()

    mysql_cursor.execute("SHOW TABLES")
    tables = [row[0] for row in mysql_cursor.fetchall()]

    for table in tables:
        print(f"Migrating data: {table}")

        mysql_cursor.execute(f"SELECT * FROM `{table}`")
        rows = mysql_cursor.fetchall()

        if not rows:
            continue

        output = StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

        for row in rows:
            writer.writerow(row)

        output.seek(0)

        pg_cursor.copy_expert(
            f'COPY "{schema_name}"."{table}" FROM STDIN WITH CSV NULL \'\'',
            output
        )

    pg_conn.commit()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()

    print("Data migration complete.")


def verify_counts():
    print("\n=== Verifying Row Counts ===")

    schema_name = PG_CONFIG["schema"]

    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()

    pg_conn = psycopg2.connect(
        host=PG_CONFIG["host"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        dbname=PG_CONFIG["dbname"]
    )
    pg_cursor = pg_conn.cursor()

    mysql_cursor.execute("SHOW TABLES")
    tables = [row[0] for row in mysql_cursor.fetchall()]

    success = 0
    mismatch = 0

    for table in tables:
        mysql_cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        mysql_count = mysql_cursor.fetchone()[0]

        pg_cursor.execute(
            f'SELECT COUNT(*) FROM "{schema_name}"."{table}"'
        )
        pg_count = pg_cursor.fetchone()[0]

        if mysql_count == pg_count:
            print(f"✅ {table}: {mysql_count}")
            success += 1
        else:
            print(f"❌ {table}: MySQL={mysql_count}, PG={pg_count}")
            mismatch += 1

    print("\nSummary:")
    print(f"Matched: {success}")
    print(f"Mismatched: {mismatch}")

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()


if __name__ == "__main__":
    migrate_schema()
    migrate_data()
    verify_counts()
import mysql.connector
import psycopg2

# ---------- CONFIG ----------
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
    "dbname": "cyber_users"
}


# ----------------------------

def get_mysql_counts():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        counts[table] = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return counts


def get_pg_counts():
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public';
    """)

    tables = [row[0] for row in cursor.fetchall()]

    counts = {}
    for table in tables:
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        counts[table] = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return counts


def compare_counts(mysql_counts, pg_counts):
    print("\n========== MIGRATION REPORT ==========\n")

    all_tables = set(mysql_counts.keys()) | set(pg_counts.keys())

    success = 0
    mismatch = 0
    missing = 0

    for table in sorted(all_tables):
        mysql_count = mysql_counts.get(table)
        pg_count = pg_counts.get(table)

        if mysql_count is None:
            print(f"⚠️  Table {table} exists in PostgreSQL but not in MySQL")
            missing += 1
        elif pg_count is None:
            print(f"❌ Table {table} missing in PostgreSQL")
            missing += 1
        elif mysql_count == pg_count:
            print(f"✅ {table}: {mysql_count} rows (MATCH)")
            success += 1
        else:
            print(f"❌ {table}: MySQL={mysql_count}, PG={pg_count} (MISMATCH)")
            mismatch += 1

    print("\n======================================")
    print(f"Total Tables: {len(all_tables)}")
    print(f"Matched: {success}")
    print(f"Mismatched: {mismatch}")
    print(f"Missing: {missing}")
    print("======================================\n")


if __name__ == "__main__":
    mysql_counts = get_mysql_counts()
    pg_counts = get_pg_counts()
    compare_counts(mysql_counts, pg_counts)
"""Quick diagnostic: show members of specific groups."""
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "172.105.61.195"),
    "user": os.getenv("DB_USER", "intern"),
    "password": os.getenv("DB_PASSWORD", "intern@infollion"),
    "database": os.getenv("DB_NAME", "dedup_infollion"),
}

conn = mysql.connector.connect(**DB_CONFIG)
cur = conn.cursor()

# Check specific groups
groups_to_check = {
    "Commercial Vehicles (16524)": 16524,
    "Electric Vehicles (6530)": 6530,
    "Medical Devices (26631)": 26631,
    "ONGC (109585)": 109585,
    "Aditya Birla Fashion (345119)": 345119,
    "M&M (107021)": 107021,
    "Reliance Retail/Jio (209029)": 209029,
}

for label, gid in groups_to_check.items():
    cur.execute("""
        SELECT group_id, COUNT(*) FROM balanced_result
        WHERE group_id = %s AND is_valid = 1
    """, (gid,))
    row = cur.fetchone()
    if not row or not row[1]:
        # Group might have been re-assigned, search by company_name
        print(f"\n=== {label}: group {gid} not found or empty ===")
        continue
    count = row[1]
    print(f"\n=== {label}: {count} members ===")
    cur.execute("""
        SELECT company_name, source_type, is_primary
        FROM balanced_result WHERE group_id = %s AND is_valid = 1
        ORDER BY is_primary DESC, company_name
        LIMIT 30
    """, (gid,))
    for name, stype, pri in cur.fetchall():
        mark = " ★" if pri else ""
        print(f"  [{stype:>10}] {name[:80]}{mark}")
    if count > 30:
        print(f"  ... and {count - 30} more")

conn.close()

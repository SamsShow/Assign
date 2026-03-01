import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST", "172.105.61.195"),
    user=os.getenv("DB_USER", "intern"),
    password=os.getenv("DB_PASSWORD", "intern@infollion"),
    database=os.getenv("DB_NAME", "dedup_infollion"),
)
cur = conn.cursor()

cur.execute("SELECT COUNT(DISTINCT group_id) FROM balanced_result WHERE is_valid=1")
groups = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM balanced_result WHERE is_valid=1 AND is_primary=1")
primaries = cur.fetchone()[0]

print(f"groups_valid: {groups:,}")
print(f"primaries_valid: {primaries:,}")

cur.close()
conn.close()

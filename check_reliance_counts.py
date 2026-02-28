from dotenv import dotenv_values
import mysql.connector

cfg = dotenv_values('/Users/samshow/Projects/Assign/.env')
conn = mysql.connector.connect(
    host=cfg.get('DB_HOST'),
    user=cfg.get('DB_USER'),
    password=cfg.get('DB_PASSWORD'),
    database=cfg.get('DB_NAME'),
)
cur = conn.cursor()

queries = [
    ("masters all types (reliance)", "SELECT COUNT(*) FROM masters WHERE label LIKE '%reliance%'"),
    ("masters Company only (reliance)", "SELECT COUNT(*) FROM masters WHERE type='Company' AND label LIKE '%reliance%'"),
    ("masters non-Company only (reliance)", "SELECT COUNT(*) FROM masters WHERE (type<>'Company' OR type IS NULL) AND label LIKE '%reliance%'"),
    ("aggressive_filter (reliance)", "SELECT COUNT(*) FROM aggressive_filter WHERE company_name LIKE '%reliance%' OR original_name LIKE '%reliance%'"),
    ("aggressive_filter valid only", "SELECT COUNT(*) FROM aggressive_filter WHERE is_valid=1 AND (company_name LIKE '%reliance%' OR original_name LIKE '%reliance%')"),
    ("aggressive_filter invalid only", "SELECT COUNT(*) FROM aggressive_filter WHERE is_valid=0 AND (company_name LIKE '%reliance%' OR original_name LIKE '%reliance%')"),
]

for name, q in queries:
    cur.execute(q)
    print(f"{name}: {cur.fetchone()[0]}")

print("\nmasters reliance by type:")
cur.execute("SELECT type, COUNT(*) FROM masters WHERE label LIKE '%reliance%' GROUP BY type ORDER BY COUNT(*) DESC")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()

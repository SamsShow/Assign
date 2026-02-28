"""Quick DB exploration - part 2."""
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME'),
    connection_timeout=60
)
cur = conn.cursor()

# Company count
print("Counting companies...")
cur.execute("SELECT COUNT(*) FROM masters WHERE type = 'Company'")
print(f'Company rows: {cur.fetchone()[0]}')

# Type distribution
print("\nType distribution...")
cur.execute('SELECT type, COUNT(*) FROM masters GROUP BY type ORDER BY COUNT(*) DESC LIMIT 10')
for r in cur.fetchall():
    print(r)

# Sample companies
print("\nSample companies...")
cur.execute("SELECT id, label FROM masters WHERE type = 'Company' ORDER BY id LIMIT 25")
for r in cur.fetchall():
    print(r)

# Check existing dedup status
print("\nExisting dedup status...")
cur.execute("SELECT duplicate_status, COUNT(*) FROM masters WHERE type = 'Company' GROUP BY duplicate_status")
for r in cur.fetchall():
    print(r)

conn.close()
print("\nDone.")

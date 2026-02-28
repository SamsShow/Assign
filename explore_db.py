"""Quick DB exploration script."""
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME')
)
cur = conn.cursor()

# Show tables
cur.execute('SHOW TABLES')
print('=== TABLES ===')
for r in cur.fetchall():
    print(r)

# Describe masters
cur.execute('DESCRIBE masters')
print('\n=== MASTERS SCHEMA ===')
for r in cur.fetchall():
    print(r)

# Count total rows and companies
cur.execute('SELECT COUNT(*) FROM masters')
print(f'\nTotal rows: {cur.fetchone()[0]}')

cur.execute("SELECT COUNT(*) FROM masters WHERE type = 'Company'")
print(f'Company rows: {cur.fetchone()[0]}')

# Sample data
cur.execute("SELECT id, label, type FROM masters WHERE type = 'Company' LIMIT 20")
print('\n=== SAMPLE COMPANY ROWS ===')
for r in cur.fetchall():
    print(r)

# Check distinct types
cur.execute('SELECT type, COUNT(*) FROM masters GROUP BY type')
print('\n=== TYPE DISTRIBUTION ===')
for r in cur.fetchall():
    print(r)

# Check if dedup columns already exist
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dedup_infollion' AND TABLE_NAME='masters' AND COLUMN_NAME='duplicate_status'")
rows = cur.fetchall()
print(f'\nduplicate_status column exists: {len(rows) > 0}')

conn.close()

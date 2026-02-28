"""Quick audit: find clusters that should probably merge but didn't."""
from dotenv import dotenv_values
import mysql.connector

cfg = dotenv_values('.env')
conn = mysql.connector.connect(
    host=cfg['DB_HOST'], user=cfg['DB_USER'],
    password=cfg['DB_PASSWORD'], database=cfg['DB_NAME'],
)
cur = conn.cursor()

# 1. How many singleton groups exist?
cur.execute("""
SELECT COUNT(*) FROM (
    SELECT group_id, COUNT(*) as c FROM aggressive_filter_all 
    WHERE is_valid=1 GROUP BY group_id HAVING c = 1
) t
""")
print("Singleton groups (valid):", cur.fetchone()[0])

# 2. Sample of probable missed merges: same first 8+ chars but different groups
cur.execute("""
SELECT a.group_id, b.group_id, a.company_name, b.company_name
FROM aggressive_filter_all a
JOIN aggressive_filter_all b 
  ON LEFT(a.company_name, 10) = LEFT(b.company_name, 10)
  AND a.group_id < b.group_id
  AND a.is_valid = 1 AND b.is_valid = 1
  AND a.is_primary = 1 AND b.is_primary = 1
LIMIT 30
""")
print("\nPossibly missed merges (same 10-char prefix, diff groups):")
for r in cur.fetchall():
    print(f"  G{r[0]} '{r[2][:50]}' vs G{r[1]} '{r[3][:50]}'")

# 3. LinkedIn artifacts still present
cur.execute("""
SELECT company_name, COUNT(*) FROM aggressive_filter_all 
WHERE is_valid=1 AND (
    company_name LIKE '%Full-time%' 
    OR company_name LIKE '%Part-time%'
    OR company_name LIKE '%Contract%'
    OR company_name LIKE '%Internship%'
    OR company_name LIKE '%Seasonal%'
)
GROUP BY company_name
ORDER BY COUNT(*) DESC
LIMIT 20
""")
print("\nLinkedIn job-type artifacts still in data:")
for r in cur.fetchall():
    print(f"  '{r[0][:60]}' x{r[1]}")

# 4. Location suffixes that could be stripped
cur.execute("""
SELECT company_name FROM aggressive_filter_all 
WHERE is_valid=1 AND is_primary=1 AND (
    company_name REGEXP ', [A-Z][a-z]+$'
    OR company_name LIKE '%, India%'
    OR company_name LIKE '%, Mumbai%'
    OR company_name LIKE '%, Delhi%'
    OR company_name LIKE '%, Bangalore%'
    OR company_name LIKE '%, Chennai%'
    OR company_name LIKE '%, Pune%'
)
LIMIT 20
""")
print("\nLocation suffixes that could be stripped:")
for r in cur.fetchall():
    print(f"  '{r[0][:70]}'")

# 5. Parenthetical content
cur.execute("""
SELECT company_name FROM aggressive_filter_all 
WHERE is_valid=1 AND company_name LIKE '%(%'
LIMIT 20
""")
print("\nParenthetical content:")
for r in cur.fetchall():
    print(f"  '{r[0][:70]}'")

# 6. Count labels with "· " mid-string (LinkedIn format: "Company · Full-time")
cur.execute("""
SELECT COUNT(*) FROM aggressive_filter_all 
WHERE is_valid=1 AND original_name LIKE '%·%'
""")
print(f"\nRows with · in original name: {cur.fetchone()[0]}")

cur.execute("""
SELECT original_name FROM aggressive_filter_all 
WHERE is_valid=1 AND original_name LIKE '%·%'
LIMIT 20
""")
print("Samples:")
for r in cur.fetchall():
    print(f"  '{r[0][:80]}'")

conn.close()

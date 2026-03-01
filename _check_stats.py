#!/usr/bin/env python3
"""Quick check of balanced_result table stats."""
import mysql.connector
from dotenv import dotenv_values
import os

cfg = dotenv_values(os.path.join(os.path.dirname(__file__), ".env"))
c = mysql.connector.connect(
    host=cfg['DB_HOST'], user=cfg['DB_USER'],
    password=cfg['DB_PASSWORD'], database=cfg['DB_NAME'],
    connection_timeout=30
)
cur = c.cursor()

cur.execute('SELECT COUNT(*) FROM balanced_result')
total = cur.fetchone()[0]
print(f'balanced_result rows: {total:,}')

cur.execute('SELECT COUNT(*) FROM balanced_result WHERE is_valid=1')
valid = cur.fetchone()[0]
print(f'valid: {valid:,}')

cur.execute('SELECT COUNT(*) FROM balanced_result WHERE is_valid=0')
garbage = cur.fetchone()[0]
print(f'garbage: {garbage:,}')

cur.execute('SELECT COUNT(*) FROM balanced_result WHERE is_primary=1 AND is_valid=1')
pri = cur.fetchone()[0]
print(f'primaries: {pri:,}')

dedup = (valid - pri) / valid * 100 if valid else 0
print(f'dedup rate: {dedup:.1f}%')

# Top 15 largest groups
print(f'\n--- Top 15 largest groups ---')
cur.execute("""
    SELECT group_id, COUNT(*) as cnt
    FROM balanced_result WHERE is_valid=1
    GROUP BY group_id ORDER BY cnt DESC LIMIT 15
""")
for gid, cnt in cur.fetchall():
    cur.execute(
        "SELECT company_name FROM balanced_result WHERE group_id=%s AND is_primary=1 LIMIT 1",
        (gid,)
    )
    row = cur.fetchone()
    name = row[0] if row else "?"
    print(f'  group {gid}: {cnt} members - {name[:70]}')

# Check previously-bad groups
print(f'\n--- Previously problematic groups check ---')
bad_keywords = [
    'consulting', 'hospital', 'bearings', 'mutual fund', 'stock exchange',
    'machine tools', 'medical devices', 'commercial vehicle',
    'electric vehicle', 'engineering works', 'financial services',
    'small finance bank', 'regional manager'
]
for kw in bad_keywords:
    cur.execute("""
        SELECT group_id, COUNT(*) as cnt
        FROM balanced_result
        WHERE company_name LIKE %s AND is_valid=1
        GROUP BY group_id ORDER BY cnt DESC LIMIT 1
    """, (f'%{kw}%',))
    row = cur.fetchone()
    if row:
        print(f'  "{kw}": largest group = {row[1]} members (group {row[0]})')
    else:
        print(f'  "{kw}": no matches')

# Conglomerate check
print(f'\n--- Conglomerate splits ---')
for kw in ['cholamandalam', 'reliance', 'tata', 'vodafone', 'birla']:
    cur.execute("""
        SELECT COUNT(DISTINCT group_id)
        FROM balanced_result
        WHERE (company_name LIKE %s OR original_name LIKE %s) AND is_valid=1
    """, (f'%{kw}%', f'%{kw}%'))
    groups = cur.fetchone()[0]
    cur.execute("""
        SELECT group_id, COUNT(*) as cnt
        FROM balanced_result
        WHERE (company_name LIKE %s OR original_name LIKE %s) AND is_valid=1
        GROUP BY group_id ORDER BY cnt DESC LIMIT 1
    """, (f'%{kw}%', f'%{kw}%'))
    row = cur.fetchone()
    biggest = row[1] if row else 0
    print(f'  "{kw}": {groups} groups, largest = {biggest} members')

c.close()

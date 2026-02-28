#!/usr/bin/env python3
"""Quick verification of improved_result grouping."""
import mysql.connector
from dotenv import dotenv_values
import os

cfg = dotenv_values(os.path.join(os.path.dirname(__file__), '.env'))
conn = mysql.connector.connect(
    host=cfg['DB_HOST'], user=cfg['DB_USER'],
    password=cfg['DB_PASSWORD'], database=cfg['DB_NAME'],
)
cur = conn.cursor()

# 1. Check Titan Industry (159933)
cur.execute("SELECT master_id, company_name, group_id, is_primary FROM improved_result WHERE master_id = 159933")
row = cur.fetchone()
print(f"Titan Industry (159933): group_id={row[2]}, is_primary={row[3]}")

# Show full group
cur.execute(
    "SELECT master_id, company_name, source_type, is_primary "
    "FROM improved_result WHERE group_id = %s ORDER BY is_primary DESC",
    (row[2],),
)
print(f"\nFull Titan Industries group ({row[2]}):")
for mid, name, st, pri in cur.fetchall():
    mk = " ★" if pri else ""
    print(f"  [{st:>10}] ID={mid:>6}  {name[:65]}{mk}")

# 2. Check all Reliance Industries entries
print("\n--- Reliance Industries entries ---")
cur.execute(
    "SELECT master_id, company_name, group_id, is_primary "
    "FROM improved_result "
    "WHERE company_name LIKE '%Reliance Industries%' AND is_valid=1 "
    "ORDER BY group_id, is_primary DESC LIMIT 30"
)
for mid, name, gid, pri in cur.fetchall():
    mk = " ★" if pri else ""
    print(f"  group={gid:>6} ID={mid:>6}  {name[:55]}{mk}")

# 3. Check actual master_ids from Titan screenshot
print("\n--- Titan screenshot master_ids ---")
for check_id in [58528, 71365, 159933, 199373, 24030, 97645, 186775, 191415, 222955, 185545, 109635, 203707]:
    cur.execute(
        "SELECT master_id, company_name, group_id, is_primary "
        "FROM improved_result WHERE master_id = %s", (check_id,)
    )
    row = cur.fetchone()
    if row:
        mk = " ★" if row[3] else ""
        print(f"  ID={row[0]:>6} group={row[2]:>6}  {row[1][:60]}{mk}")
    else:
        print(f"  ID={check_id} NOT FOUND")

# 4. Check actual master_ids from Reliance screenshot
print("\n--- Reliance screenshot master_ids ---")
for check_id in [360717, 355827, 321028, 181751, 170283, 131083, 333691, 149326, 143907, 133752, 126474, 122952, 196544, 110723, 66658, 65440, 59303, 18726, 298572]:
    cur.execute(
        "SELECT master_id, company_name, group_id, is_primary "
        "FROM improved_result WHERE master_id = %s", (check_id,)
    )
    row = cur.fetchone()
    if row:
        mk = " ★" if row[3] else ""
        print(f"  ID={row[0]:>6} group={row[2]:>6}  {row[1][:60]}{mk}")
    else:
        print(f"  ID={check_id} NOT FOUND")

conn.close()

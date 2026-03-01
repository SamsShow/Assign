#!/usr/bin/env python3
"""Quick verification of previously-problematic groups after rebuild."""
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

# 1. Check PREVIOUSLY GENERIC groups that should now be split
print("=== GENERIC TERM GROUPS (should be FIXED) ===\n")
generic_checks = [
    (2594, "Consulting (was 220)"),
    (363597, "Independent Consultant (was 137)"),
    (177760, "Hospital (was 97)"),
    (26631, "Medical Devices (was 70)"),
    (177701, "Machine Tools (was 68)"),
    (4105, "Financial Services (was 62)"),
    (165521, "Mutual Fund (was 56)"),
    (91311, "Bearings (was 51)"),
    (102795, "Stock Exchange (was 47)"),
    (16524, "Commercial Vehicles (was 52)"),
    (6530, "Electric Vehicles (was 53)"),
    (65881, "Engineering Works (was 44)"),
    (121361, "Small Finance Bank (was 45)"),
    (229524, "Regional Manager (was 46)"),
    (199168, "Facilities Services (was 50)"),
    (76316, "Drugs & Pharmaceuticals (was 57)"),
]
for gid, desc in generic_checks:
    cur.execute(
        "SELECT COUNT(*) FROM balanced_result WHERE group_id=%s AND is_valid=1",
        (gid,),
    )
    cnt = cur.fetchone()[0]
    status = "FIXED" if cnt <= 15 else f"STILL BIG ({cnt})"
    print(f"  Group {gid} [{desc}]: now {cnt} members → {status}")

# 2. Check entity-mixing groups that may still be problematic
print("\n=== ENTITY-MIXING GROUPS (may still need work) ===\n")
entity_checks = [
    (172040, "AT&T (was 164)"),
    (345119, "Aditya Birla (was 188)"),
    (176135, "Vodafone/Idea (was 136)"),
    (74004, "UL Technology (was 120)"),
    (94864, "Electronics Corp India / LG (was 75)"),
    (116599, "State Bank Mauritius / Bank of Baroda (was 46)"),
    (31114, "Sony / Ericsson (was 47)"),
    (124549, "J&J / H&R Johnson (was 77)"),
    (109585, "ONGC / GE Oil & Gas (was 47)"),
    (102576, "Fortis / mixed hospitals (was 63)"),
]
for gid, desc in entity_checks:
    cur.execute(
        "SELECT COUNT(*) FROM balanced_result WHERE group_id=%s AND is_valid=1",
        (gid,),
    )
    cnt = cur.fetchone()[0]
    cur.execute(
        "SELECT company_name FROM balanced_result WHERE group_id=%s AND is_primary=1 LIMIT 1",
        (gid,),
    )
    row = cur.fetchone()
    pri = row[0][:60] if row else "?"
    status = "OK" if cnt <= 30 else f"STILL BIG"
    print(f"  Group {gid} [{desc}]: now {cnt} members → {status}")
    print(f"    Primary: {pri}")
    # Show sample of different-looking names
    cur.execute("""
        SELECT company_name FROM balanced_result
        WHERE group_id=%s AND is_valid=1 AND is_primary=0
        ORDER BY company_name LIMIT 5
    """, (gid,))
    for (name,) in cur.fetchall():
        print(f"      - {name[:70]}")

# 3. Top 10 largest groups now
print("\n=== TOP 15 LARGEST GROUPS (current) ===\n")
cur.execute("""
    SELECT group_id, COUNT(*) as cnt
    FROM balanced_result WHERE is_valid=1
    GROUP BY group_id ORDER BY cnt DESC LIMIT 15
""")
for gid, cnt in cur.fetchall():
    cur.execute(
        "SELECT company_name FROM balanced_result WHERE group_id=%s AND is_primary=1 LIMIT 1",
        (gid,),
    )
    row = cur.fetchone()
    name = row[0] if row else "?"
    print(f"  group {gid}: {cnt} members - {name[:70]}")

c.close()

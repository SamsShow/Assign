"""
Extract groups from balanced_result for manual review.
Focus on: large groups, major conglomerates, and cross-check primaries.
"""
import mysql.connector
from dotenv import dotenv_values
import os

cfg = dotenv_values(os.path.join(os.path.dirname(__file__), '.env'))
conn = mysql.connector.connect(
    host=cfg['DB_HOST'], user=cfg['DB_USER'], password=cfg['DB_PASSWORD'],
    database=cfg['DB_NAME'], connection_timeout=120,
)
cur = conn.cursor()

# Part 1: All groups with 5+ members
print("=== GROUPS WITH 5+ MEMBERS (for manual review) ===\n")
cur.execute("""
    SELECT group_id, COUNT(*) AS cnt
    FROM balanced_result
    WHERE is_valid=1
    GROUP BY group_id
    HAVING cnt >= 5
    ORDER BY cnt DESC
""")
big_groups = cur.fetchall()
print(f"Total groups with 5+ members: {len(big_groups)}\n")

for gid, cnt in big_groups[:100]:  # top 100 biggest
    cur.execute("""
        SELECT master_id, company_name, is_primary
        FROM balanced_result
        WHERE group_id=%s AND is_valid=1
        ORDER BY is_primary DESC, company_name
    """, (gid,))
    rows = cur.fetchall()
    pri_name = next((r[1] for r in rows if r[2] == 1), '?')
    print(f"GROUP {gid} ({cnt} members) — primary: {pri_name}")
    for mid, name, pri in rows[:15]:
        mark = ' *' if pri == 1 else '  '
        print(f"  {mark} {mid:>7}  {name[:80]}")
    if cnt > 15:
        print(f"    ... +{cnt - 15} more")
    print()

# Part 2: Key conglomerates — check all groups touching these keywords
print("\n\n=== CONGLOMERATE DEEP AUDIT ===\n")
keywords = [
    'reliance', 'tata', 'mahindra', 'birla', 'godrej', 'bajaj',
    'larsen', 'adani', 'wipro', 'infosys', 'hdfc', 'icici',
    'kotak', 'axis', 'bharti', 'airtel', 'vodafone', 'idea',
    'hero', 'honda', 'maruti', 'suzuki', 'hyundai',
    'samsung', 'lg', 'siemens', 'bosch',
    'hindustan', 'unilever', 'nestle', 'pepsi', 'coca',
    'asian', 'paints', 'pidilite', 'ultratech',
    'sun pharma', 'cipla', 'lupin', 'dr.? reddy',
    'cholamandalam', 'muthoot', 'manappuram',
    'raymond', 'titan', 'tanishq',
    'vedanta', 'hindalco', 'grasim', 'jsw',
    'Power Grid', 'NTPC', 'BHEL', 'ONGC', 'IOC',
    'SBI', 'Bank of Baroda', 'PNB', 'Canara',
]

for kw in keywords:
    cur.execute("""
        SELECT group_id, COUNT(*) AS cnt
        FROM balanced_result
        WHERE is_valid=1 AND (company_name LIKE %s OR original_name LIKE %s)
        GROUP BY group_id
        ORDER BY cnt DESC
        LIMIT 10
    """, (f'%{kw}%', f'%{kw}%'))
    groups = cur.fetchall()
    if not groups:
        continue
    total_rows = sum(c for _, c in groups)
    print(f"--- '{kw}' ({len(groups)} groups, {total_rows} total rows) ---")
    for gid, cnt in groups:
        cur.execute("""
            SELECT master_id, company_name, is_primary
            FROM balanced_result
            WHERE group_id=%s AND is_valid=1
            ORDER BY is_primary DESC, company_name
            LIMIT 8
        """, (gid,))
        rows = cur.fetchall()
        pri_name = next((r[1] for r in rows if r[2] == 1), '?')
        print(f"  Group {gid} ({cnt}): {pri_name[:65]}")
        for mid, name, pri in rows[:4]:
            if pri != 1:
                print(f"      {mid:>7}  {name[:70]}")
        if cnt > 4:
            print(f"      ... +{cnt - 4} more")
    print()

cur.close()
conn.close()

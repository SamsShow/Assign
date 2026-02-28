#!/usr/bin/env python3
"""
Build the `final_result` table — a COMPLETE view of all Company rows
with group assignments.

Unlike `dedup_results` (which only contains rows that are part of a
duplicate group), this table includes EVERY Company row from `masters`,
including singletons (companies with no duplicates).

Columns:
    id              - auto-increment PK
    company_name    - the original label from masters
    master_id       - the record's ID in the masters table
    group_id        - shared by all rows in the same duplicate group;
                      singletons get their own unique group_id
                      (= the primary record's masters.id)
    is_primary      - 1 if this row is the elected canonical record
                      for its group, 0 otherwise.
                      Exactly ONE row per group_id has is_primary = 1.

Usage:
    python3 build_final_result.py

Interpretation:
    - To see deduplicated companies:
        SELECT * FROM final_result WHERE is_primary = 1;
    - To see all raw rows with their group:
        SELECT * FROM final_result WHERE company_name LIKE '%reliance%';
    - To count unique companies:
        SELECT COUNT(DISTINCT group_id) FROM final_result;
"""

import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connection_timeout": 120,
}


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── 1. Create the table ──────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS final_result (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            master_id   INT NOT NULL,
            group_id    INT NOT NULL,
            is_primary  TINYINT(1) NOT NULL DEFAULT 0,
            INDEX idx_company_name (company_name(100)),
            INDEX idx_group_id (group_id),
            INDEX idx_master_id (master_id),
            INDEX idx_is_primary (is_primary)
        )
    """)
    conn.commit()
    print("Table `final_result` ready.")

    # Clear previous data (use DELETE — no DROP privilege)
    cur.execute("DELETE FROM final_result")
    conn.commit()

    # ── 2. Insert DUPLICATE rows ─────────────────────────────────
    #    These have duplicate_status = 'duplicate' and point to a primary
    #    via duplicate_of.  group_id = duplicate_of (the primary's id).
    cur.execute("""
        INSERT INTO final_result (company_name, master_id, group_id, is_primary)
        SELECT label, id, duplicate_of, 0
        FROM masters
        WHERE type = 'Company'
          AND duplicate_status = 'duplicate'
          AND duplicate_of IS NOT NULL
    """)
    dup_count = cur.rowcount
    conn.commit()
    print(f"  Inserted {dup_count} duplicate rows.")

    # ── 3. Insert PRIMARY rows (those that head a group) ─────────
    #    These have duplicate_status = 'primary'.
    #    group_id = their own id.
    cur.execute("""
        INSERT INTO final_result (company_name, master_id, group_id, is_primary)
        SELECT label, id, id, 1
        FROM masters
        WHERE type = 'Company'
          AND duplicate_status = 'primary'
    """)
    pri_count = cur.rowcount
    conn.commit()
    print(f"  Inserted {pri_count} primary rows.")

    # ── 4. Insert PROBABLE rows ──────────────────────────────────
    #    These were flagged as probable but not confirmed.
    #    They have duplicate_status = 'probable'.
    #    If they have a duplicate_of, use that as group_id and mark
    #    not primary.  Otherwise treat them as their own group (singleton).
    cur.execute("""
        INSERT INTO final_result (company_name, master_id, group_id, is_primary)
        SELECT label, id,
               COALESCE(duplicate_of, id),
               CASE WHEN duplicate_of IS NULL THEN 1 ELSE 0 END
        FROM masters
        WHERE type = 'Company'
          AND duplicate_status = 'probable'
    """)
    prob_count = cur.rowcount
    conn.commit()
    print(f"  Inserted {prob_count} probable rows.")

    # ── 5. Insert SINGLETON rows (no duplicate_status) ───────────
    #    These were never part of any duplicate group.
    #    Each gets its own group_id (= its own id) and is_primary = 1.
    cur.execute("""
        INSERT INTO final_result (company_name, master_id, group_id, is_primary)
        SELECT label, id, id, 1
        FROM masters
        WHERE type = 'Company'
          AND (duplicate_status IS NULL OR duplicate_status = '')
    """)
    sing_count = cur.rowcount
    conn.commit()
    print(f"  Inserted {sing_count} singleton rows.")

    # ── 6. Verify every group has exactly one primary ────────────
    cur.execute("""
        SELECT group_id, COUNT(*) as pri_count
        FROM final_result
        WHERE is_primary = 1
        GROUP BY group_id
        HAVING pri_count != 1
        LIMIT 10
    """)
    bad_groups = cur.fetchall()
    if bad_groups:
        print(f"\n  WARNING: {len(bad_groups)} groups don't have exactly 1 primary!")
        for gid, cnt in bad_groups[:5]:
            print(f"    group_id={gid}, primary_count={cnt}")
    else:
        print("\n  Validation passed: every group has exactly 1 primary.")

    # ── 7. Summary stats ─────────────────────────────────────────
    total = dup_count + pri_count + prob_count + sing_count
    cur.execute("SELECT COUNT(*) FROM masters WHERE type = 'Company'")
    master_total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT group_id) FROM final_result")
    unique_companies = cur.fetchone()[0]

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"  Total Company rows in masters:      {master_total}")
    print(f"  Total rows in final_result:          {total}")
    print(f"  Match:                               {'YES' if total == master_total else 'NO — gap of ' + str(master_total - total)}")
    print(f"  Unique companies (distinct groups):  {unique_companies}")
    print(f"  Reduction:                           {master_total - unique_companies} duplicates removed")
    print(f"                                       ({(master_total - unique_companies) / master_total * 100:.1f}% dedup rate)")

    # ── 8. Demo: search "reliance" ───────────────────────────────
    keyword = "reliance"
    cur.execute(
        "SELECT COUNT(*) FROM masters WHERE type='Company' AND label LIKE %s",
        (f"%{keyword}%",),
    )
    master_hits = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM final_result WHERE company_name LIKE %s",
        (f"%{keyword}%",),
    )
    fr_hits = cur.fetchone()[0]

    print(f"\n{'='*60}")
    print(f"VERIFICATION: Search '{keyword}'")
    print(f"{'='*60}")
    print(f"  masters table:       {master_hits} rows")
    print(f"  final_result table:  {fr_hits} rows")
    print(f"  Match:               {'YES' if master_hits == fr_hits else 'NO'}")

    # Show a sample of the grouped results
    print(f"\n  Sample grouped results for '{keyword}':")
    cur.execute("""
        SELECT group_id, company_name, master_id, is_primary
        FROM final_result
        WHERE company_name LIKE %s
        ORDER BY group_id, is_primary DESC
        LIMIT 30
    """, (f"%{keyword}%",))
    rows = cur.fetchall()
    current_group = None
    for gid, name, mid, is_pri in rows:
        if gid != current_group:
            current_group = gid
            print(f"\n    --- Group {gid} ---")
        marker = " ★ PRIMARY" if is_pri else ""
        print(f"      ID={mid:>6}  {name[:65]}{marker}")

    # ── 9. Useful queries ────────────────────────────────────────
    print(f"\n{'='*60}")
    print("USEFUL QUERIES")
    print(f"{'='*60}")
    print("  -- All rows for a keyword (same count as masters):")
    print("  SELECT * FROM final_result WHERE company_name LIKE '%reliance%';")
    print()
    print("  -- Deduplicated view (one row per company):")
    print("  SELECT * FROM final_result WHERE is_primary = 1 AND company_name LIKE '%reliance%';")
    print()
    print("  -- All members of a specific group:")
    print("  SELECT * FROM final_result WHERE group_id = <some_group_id> ORDER BY is_primary DESC;")
    print()
    print("  -- Count unique companies after dedup:")
    print("  SELECT COUNT(DISTINCT group_id) FROM final_result;")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

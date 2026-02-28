#!/usr/bin/env python3
"""
Build the dedup_results table for easy search and comparison.

Run after dedup.py. Creates a table with one row per company in each duplicate group,
so you can search by keyword (e.g. "Tata") and compare original vs deduplicated view.

Usage:
    python3 build_dedup_results_table.py
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
    "connection_timeout": 60,
}


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create dedup_results table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dedup_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            primary_id INT NOT NULL,
            primary_label VARCHAR(255),
            member_id INT NOT NULL,
            member_label VARCHAR(255),
            member_role ENUM('primary','duplicate','probable') NOT NULL,
            duplicate_score DECIMAL(5,3) NULL,
            ai_decision TEXT NULL,
            record_type ENUM('new','old') DEFAULT 'old',
            INDEX idx_primary_label (primary_label(100)),
            INDEX idx_member_label (member_label(100)),
            INDEX idx_primary_id (primary_id),
            INDEX idx_member_id (member_id)
        )
    """)
    conn.commit()
    print("Table dedup_results ready.")

    # Clear and repopulate (use DELETE — TRUNCATE requires DROP privilege)
    cur.execute("DELETE FROM dedup_results")
    conn.commit()

    # Insert primary records
    cur.execute("""
        INSERT INTO dedup_results (primary_id, primary_label, member_id, member_label, member_role, duplicate_score, ai_decision, record_type)
        SELECT id, label, id, label, 'primary', NULL, NULL, record_type
        FROM masters
        WHERE type = 'Company' AND duplicate_status = 'primary'
    """)
    p_count = cur.rowcount

    # Insert duplicate records (with primary label from join)
    cur.execute("""
        INSERT INTO dedup_results (primary_id, primary_label, member_id, member_label, member_role, duplicate_score, ai_decision, record_type)
        SELECT d.duplicate_of, p.label, d.id, d.label, 'duplicate', d.duplicate_score, d.ai_decision, d.record_type
        FROM masters d
        JOIN masters p ON d.duplicate_of = p.id
        WHERE d.type = 'Company' AND d.duplicate_status = 'duplicate'
    """)
    d_count = cur.rowcount

    # Insert probable records
    cur.execute("""
        INSERT INTO dedup_results (primary_id, primary_label, member_id, member_label, member_role, duplicate_score, ai_decision, record_type)
        SELECT id, label, id, label, 'probable', duplicate_score, ai_decision, record_type
        FROM masters
        WHERE type = 'Company' AND duplicate_status = 'probable'
    """)
    pr_count = cur.rowcount

    conn.commit()
    print(f"Populated: {p_count} primary, {d_count} duplicate, {pr_count} probable rows.")

    # Example: search for "Tata"
    keyword = "Tata"
    print(f"\n--- Example: Search '{keyword}' ---\n")

    print("ORIGINAL (masters): All records with keyword in label")
    cur.execute(
        "SELECT id, label, duplicate_status, duplicate_of FROM masters WHERE type='Company' AND label LIKE %s",
        (f"%{keyword}%",),
    )
    orig = cur.fetchall()
    for row in orig[:15]:
        print(f"  ID={row[0]}, label=\"{row[1][:60]}...\" status={row[2]} dup_of={row[3]}")
    if len(orig) > 15:
        print(f"  ... and {len(orig) - 15} more")
    print(f"  Total: {len(orig)} rows\n")

    print("DEDUP RESULTS: Same keyword, grouped view")
    cur.execute(
        "SELECT primary_id, primary_label, member_id, member_label, member_role, duplicate_score FROM dedup_results WHERE primary_label LIKE %s OR member_label LIKE %s ORDER BY primary_id, member_role DESC",
        (f"%{keyword}%", f"%{keyword}%"),
    )
    dedup = cur.fetchall()
    for row in dedup[:20]:
        role = row[4]
        score = f", score={row[5]}" if row[5] else ""
        print(f"  Primary {row[0]}: \"{row[1][:40]}...\" | {role}: ID={row[2]} \"{row[3][:35]}...\"{score}")
    if len(dedup) > 20:
        print(f"  ... and {len(dedup) - 20} more")
    print(f"  Total: {len(dedup)} rows")

    conn.close()
    print("\nDone. Use these queries in your DB client to compare:")
    print("  -- Original: SELECT * FROM masters WHERE type='Company' AND label LIKE '%Tata%'")
    print("  -- Dedup:    SELECT * FROM dedup_results WHERE primary_label LIKE '%Tata%' OR member_label LIKE '%Tata%'")


if __name__ == "__main__":
    main()

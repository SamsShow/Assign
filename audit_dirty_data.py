#!/usr/bin/env python3
"""Audit dirty/garbage data patterns in masters table."""
from dotenv import dotenv_values
import mysql.connector

cfg = dotenv_values('/Users/samshow/Projects/Assign/.env')
conn = mysql.connector.connect(
    host=cfg.get('DB_HOST'), user=cfg.get('DB_USER'),
    password=cfg.get('DB_PASSWORD'), database=cfg.get('DB_NAME'),
    connection_timeout=120,
)
cur = conn.cursor()

print("=== GARBAGE PATTERNS ===")
garbage_queries = [
    ("label is '-' or '.' etc", "SELECT COUNT(*) FROM masters WHERE type='Company' AND TRIM(label) IN ('-','.',',','--','---','..','...')"),
    ("label is test/unknown/na", "SELECT COUNT(*) FROM masters WHERE type='Company' AND LOWER(TRIM(label)) REGEXP '^(test|unknown|n/?a|none|null|na|tbd|temp|sample|dummy|xxx|zzz|abc|asdf|qwerty|-)$'"),
    ("label <= 2 chars", "SELECT COUNT(*) FROM masters WHERE type='Company' AND CHAR_LENGTH(TRIM(label)) <= 2"),
    ("label only numbers", "SELECT COUNT(*) FROM masters WHERE type='Company' AND TRIM(label) REGEXP '^[0-9]+$'"),
    ("label only special chars", "SELECT COUNT(*) FROM masters WHERE type='Company' AND TRIM(label) REGEXP '^[^a-zA-Z0-9]+$'"),
    ("leading/trailing dots/commas", "SELECT COUNT(*) FROM masters WHERE type='Company' AND (TRIM(label) LIKE '.%' OR TRIM(label) LIKE ',%' OR TRIM(label) LIKE '%,' )"),
    ("double spaces inside", "SELECT COUNT(*) FROM masters WHERE type='Company' AND label LIKE '%  %'"),
    ("empty/whitespace", "SELECT COUNT(*) FROM masters WHERE type='Company' AND (label IS NULL OR TRIM(label)='')"),
    ("starts with bullet", "SELECT COUNT(*) FROM masters WHERE type='Company' AND TRIM(label) LIKE '%\xc2\xb7%'"),
    ("freelance/self-employed", "SELECT COUNT(*) FROM masters WHERE type='Company' AND LOWER(label) REGEXP '(freelanc|self.employ|unemploy|retired|student|homemaker|housewife|looking for)'"),
]
for name, q in garbage_queries:
    cur.execute(q)
    print(f"  {name}: {cur.fetchone()[0]}")

print("\n=== SAMPLE SHORT LABELS (<=2 chars) ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND CHAR_LENGTH(TRIM(label)) <= 2 LIMIT 30")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

print("\n=== SAMPLE SPECIAL-CHAR-ONLY LABELS ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND TRIM(label) REGEXP '^[^a-zA-Z0-9]+$' LIMIT 20")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

print("\n=== SAMPLE NUMERIC-ONLY LABELS ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND TRIM(label) REGEXP '^[0-9]+$' LIMIT 20")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

print("\n=== SAMPLE FREELANCE/SELF-EMPLOYED ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND LOWER(label) REGEXP '(freelanc|self.employ|unemploy|retired|student|homemaker|housewife|looking for)' LIMIT 20")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

print("\n=== SAMPLE LABELS ENDING WITH TRAILING COMMA ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND TRIM(label) LIKE '%,' LIMIT 15")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

print("\n=== SAMPLE LABELS STARTING WITH COMMA ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND TRIM(label) LIKE ',%' LIMIT 15")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

print("\n=== SAMPLE DOUBLE-SPACE LABELS ===")
cur.execute("SELECT DISTINCT TRIM(label) FROM masters WHERE type='Company' AND label LIKE '%  %' LIMIT 15")
for r in cur.fetchall():
    print(f"  '{r[0]}'")

cur.close()
conn.close()

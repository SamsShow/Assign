#!/usr/bin/env python3
"""
Build the `aggressive_filter` table — a cleaned, filtered, and
re-grouped version of final_result.

Improvements over final_result:
  1. NAME CLEANING:
     - Trim whitespace
     - Collapse double/triple spaces to single space
     - Strip leading/trailing commas, dots, dashes, quotes, brackets, bullets (·)
     - Remove trailing " ·" (LinkedIn artefact)
     - Normalise "&" spacing ("AT &T" → "AT&T", "AT & T" → "AT & T")

  2. GARBAGE ROW REMOVAL (flagged, kept in table with is_valid=0):
     - Empty / whitespace-only labels
     - Labels ≤ 2 characters
     - Numeric-only labels (e.g. "1047429", "360")
     - Special-character-only labels (e.g. "-", "[]", "___")
     - Placeholder labels (test, unknown, n/a, none, null, tbd, dummy, etc.)
     - Non-company entries: freelance, self-employed, unemployed, retired,
       student, homemaker, looking for job

  3. RE-GROUPING after cleaning:
     - After cleaning, names that were previously different may now match.
       We re-run dedup grouping on cleaned names so more duplicates are caught.

Columns:
    id              - auto-increment PK
    company_name    - CLEANED company name
    original_name   - original label from masters (before cleaning)
    master_id       - record ID in masters table
    group_id        - shared by all rows in the same duplicate group
    is_primary      - 1 = canonical row for the group, 0 = duplicate
    is_valid        - 1 = real company, 0 = garbage/non-company (kept for audit)
    filter_reason   - why it was flagged invalid (NULL if valid)

Usage:
    python3 build_aggressive_filter.py
"""

import os
import re
import sys
import time
from collections import defaultdict

import mysql.connector
from rapidfuzz import fuzz
from dotenv import dotenv_values

cfg = dotenv_values(os.path.join(os.path.dirname(__file__), '.env'))

DB_CONFIG = {
    "host": cfg.get("DB_HOST"),
    "user": cfg.get("DB_USER"),
    "password": cfg.get("DB_PASSWORD"),
    "database": cfg.get("DB_NAME"),
    "connection_timeout": 120,
    "autocommit": False,
}

# ─── Cleaning ────────────────────────────────────────────────────

def clean_name(label: str) -> str:
    """Aggressively clean a company name."""
    if not label:
        return ""
    s = label.strip()

    # Remove trailing LinkedIn bullet artefact " ·" or "·"
    s = re.sub(r'\s*·\s*$', '', s)
    s = re.sub(r'^\s*·\s*', '', s)

    # Strip leading/trailing commas, dots, dashes, quotes, brackets
    s = re.sub(r'^[\s,.\-\'""\[\]()·`=/#]+', '', s)
    s = re.sub(r'[\s,.\-\'""\[\]()·`=/#]+$', '', s)

    # Collapse multiple spaces to single
    s = re.sub(r'\s+', ' ', s)

    # Fix "&" spacing: "AT &T" → "AT&T" but keep "A & B" as is
    s = re.sub(r'(\w)\s+&(\w)', r'\1&\2', s)   # "AT &T" → "AT&T"
    s = re.sub(r'(\w)&\s+(\w)', r'\1& \2', s)   # keep "B& Q" → "B& Q" (rare)

    # Strip again after all changes
    s = s.strip()
    return s


# ─── Garbage detection ───────────────────────────────────────────

_PLACEHOLDER_RE = re.compile(
    r'^(test|testing|unknown|n/?a|none|null|na|tbd|temp|temporary|sample|'
    r'dummy|xxx|zzz|abc|asdf|qwerty|company\s*\d*|no\s*company|not\s*applicable|'
    r'confidential|private|personal|do\s*not\s*use|delete|removed|undefined)$',
    re.IGNORECASE,
)

_NON_COMPANY_RE = re.compile(
    r'(?:^|\s|·\s*)(freelanc\w*|self[\s-]?employ\w*|unemploy\w*|'
    r'retired|retirement|student|homemaker|housewife|'
    r'looking\s+for\s+(?:job|work|opportunit|a\s+job)|'
    r'job\s+seek\w*|between\s+jobs|career\s+break|'
    r'open\s+to\s+work|actively\s+seeking)',
    re.IGNORECASE,
)

_SPECIAL_ONLY_RE = re.compile(r'^[^a-zA-Z0-9\u00C0-\u024F\u0400-\u04FF]+$')
_NUMERIC_ONLY_RE = re.compile(r'^[0-9]+$')


def classify_garbage(cleaned: str, original: str) -> str | None:
    """Return a reason string if the label is garbage/non-company, else None."""
    if not cleaned:
        return "empty"
    if len(cleaned) <= 2:
        return f"too_short ({len(cleaned)} chars)"
    if _SPECIAL_ONLY_RE.match(cleaned):
        return "special_chars_only"
    if _NUMERIC_ONLY_RE.match(cleaned):
        return "numeric_only"
    if _PLACEHOLDER_RE.match(cleaned):
        return "placeholder"
    if _NON_COMPANY_RE.search(cleaned):
        return "non_company (freelance/self-employed/student/etc)"
    return None


# ─── Normalisation for re-grouping ───────────────────────────────

LEGAL_SUFFIXES = [
    r"\bincorporated\b", r"\binc\b\.?", r"\bllc\b\.?", r"\bl\.l\.c\.?",
    r"\blimited\b", r"\bltd\b\.?", r"\bcorporation\b", r"\bcorp\b\.?",
    r"\bcompany\b", r"\bco\b\.?", r"\bplc\b\.?", r"\bgmbh\b",
    r"\bag\b", r"\bs\.?a\.?\b", r"\bn\.?v\.?\b", r"\bpvt\b\.?",
    r"\bprivate\b", r"\bl\.?p\.?\b", r"\bllp\b\.?", r"\bgroup\b",
    r"\bholdings?\b", r"\benterprise[s]?\b", r"\binternational\b",
    r"\btechnolog(?:y|ies)\b", r"\bconsulting\b", r"\badvisors?\b",
    r"\bindustries\b",
]
LEGAL_SUFFIX_RE = re.compile(
    r"(?:" + "|".join(LEGAL_SUFFIXES) + r")", re.IGNORECASE
)
STOPWORDS = {"the", "and", "of", "for", "in", "a", "an", "to", "at", "by", "on"}


def normalize(label: str) -> str:
    """Normalise for comparison."""
    if not label:
        return ""
    s = label.lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = LEGAL_SUFFIX_RE.sub("", s)
    s = re.sub(r"^the\s+", "", s)
    s = re.sub(r"\s+the$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def token_signature(normalized: str) -> str:
    tokens = [t for t in normalized.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(sorted(tokens))


def composite_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    tsr = fuzz.token_sort_ratio(a, b) / 100.0
    tse = fuzz.token_set_ratio(a, b) / 100.0
    rat = fuzz.ratio(a, b) / 100.0
    par = fuzz.partial_ratio(a, b) / 100.0
    return 0.30 * tsr + 0.30 * tse + 0.20 * rat + 0.20 * par


# ─── Union-Find ──────────────────────────────────────────────────

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1

    def groups(self):
        g = defaultdict(set)
        for x in self.parent:
            g[self.find(x)].add(x)
        return g


# ─── Primary selection ───────────────────────────────────────────

_LEGAL_SIMPLE = re.compile(
    r"\b(?:inc|ltd|llc|corp|plc|gmbh|pvt|limited|corporation|incorporated)\b",
    re.IGNORECASE,
)

def primary_score(label: str) -> float:
    score = 0.0
    if not label or len(label.strip()) < 2:
        return -10
    if label == label.upper():
        score -= 1
    elif label == label.lower():
        score -= 1
    elif label[0].isupper():
        score += 2
    if _LEGAL_SIMPLE.search(label):
        score += 1
    length = len(label.strip())
    if length >= 5:
        score += min(length / 30.0, 1.0)
    if "  " in label or label != label.strip():
        score -= 1
    return score


# ─── Main ────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=== Building aggressive_filter table ===\n")

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── 1. Create table ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aggressive_filter (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            company_name    VARCHAR(255) NOT NULL,
            original_name   VARCHAR(255),
            master_id       INT NOT NULL,
            group_id        INT NOT NULL,
            is_primary      TINYINT(1) NOT NULL DEFAULT 0,
            is_valid        TINYINT(1) NOT NULL DEFAULT 1,
            filter_reason   VARCHAR(100) NULL,
            INDEX idx_company_name (company_name(100)),
            INDEX idx_group_id (group_id),
            INDEX idx_master_id (master_id),
            INDEX idx_is_primary (is_primary),
            INDEX idx_is_valid (is_valid)
        )
    """)
    cur.execute("DELETE FROM aggressive_filter")
    conn.commit()
    print("Table ready.\n")

    # ── 2. Load all Company rows ─────────────────────────────────
    print("Loading company records...")
    cur.execute("SELECT id, label FROM masters WHERE type = 'Company'")
    rows = cur.fetchall()
    print(f"  Loaded {len(rows)} rows.\n")

    # ── 3. Clean names & classify garbage ────────────────────────
    print("Cleaning names & classifying garbage...")
    records = []   # (master_id, cleaned_name, original_name, is_valid, filter_reason)
    garbage_count = 0
    for rid, label in rows:
        original = label or ""
        cleaned = clean_name(original)
        reason = classify_garbage(cleaned, original)
        is_valid = 0 if reason else 1
        if reason:
            garbage_count += 1
        records.append((rid, cleaned, original, is_valid, reason))

    print(f"  Valid: {len(records) - garbage_count}")
    print(f"  Garbage (is_valid=0): {garbage_count}\n")

    # ── 4. Build lookup for valid records ────────────────────────
    valid_records = [(r[0], r[1]) for r in records if r[3] == 1 and r[1]]
    id_to_cleaned = {r[0]: r[1] for r in valid_records}
    id_to_norm = {rid: normalize(cleaned) for rid, cleaned in valid_records}

    # ── 5. Blocking + scoring on CLEANED names ───────────────────
    print("Blocking on cleaned names...")
    MAX_BLOCK = 500
    block_prefix = defaultdict(set)
    block_token = defaultdict(set)

    for rid, norm in id_to_norm.items():
        if len(norm) >= 3:
            block_prefix[norm[:3]].add(rid)
        tsig = token_signature(norm)
        if tsig and len(tsig.split()) > 1:
            block_token[tsig].add(rid)

    candidate_pairs = set()
    for bd in (block_prefix, block_token):
        for key, members in bd.items():
            if len(members) < 2 or len(members) > MAX_BLOCK:
                continue
            ml = sorted(members)
            for i in range(len(ml)):
                for j in range(i + 1, len(ml)):
                    candidate_pairs.add((ml[i], ml[j]))

    print(f"  Candidate pairs: {len(candidate_pairs)}")

    # ── 6. Score & merge (aggressive thresholds) ─────────────────
    # Lower threshold than before: 0.85 auto-dup, 0.70 probable (auto-accept)
    THRESH_AUTO = 0.85
    THRESH_PROBABLE = 0.70

    print("Scoring pairs...")
    uf = UnionFind()
    scored = 0

    for id_a, id_b in candidate_pairs:
        na = id_to_norm.get(id_a, "")
        nb = id_to_norm.get(id_b, "")
        if not na or not nb:
            continue

        if na == nb:
            uf.union(id_a, id_b)
            scored += 1
            continue

        sc = composite_score(na, nb)
        if sc >= THRESH_PROBABLE:
            uf.union(id_a, id_b)
        scored += 1
        if scored % 1000000 == 0:
            print(f"    Scored {scored} pairs...")

    print(f"  Scored {scored} pairs total.\n")

    # ── 7. Build groups & select primaries ───────────────────────
    print("Building groups and selecting primaries...")
    groups = uf.groups()

    # Map: master_id → group_id (primary's id)
    id_to_group = {}
    id_is_primary = {}

    for root, members in groups.items():
        # Pick best primary from cleaned names
        scored_members = [
            (mid, id_to_cleaned.get(mid, ""), primary_score(id_to_cleaned.get(mid, "")))
            for mid in members
        ]
        scored_members.sort(key=lambda x: x[2], reverse=True)
        best_id = scored_members[0][0]

        for mid, _, _ in scored_members:
            id_to_group[mid] = best_id
            id_is_primary[mid] = 1 if mid == best_id else 0

    # Singletons: valid records not in any group → group_id = self, is_primary = 1
    for rid in id_to_cleaned:
        if rid not in id_to_group:
            id_to_group[rid] = rid
            id_is_primary[rid] = 1

    multi_groups = sum(1 for _, m in groups.items() if len(m) > 1)
    print(f"  Multi-member groups: {multi_groups}")
    print(f"  Singletons: {len(id_to_cleaned) - sum(len(m) for m in groups.values())}\n")

    # ── 8. Insert into table ─────────────────────────────────────
    print("Inserting into aggressive_filter...")
    BATCH = 5000
    insert_sql = (
        "INSERT INTO aggressive_filter "
        "(company_name, original_name, master_id, group_id, is_primary, is_valid, filter_reason) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )

    batch_data = []
    for rid, cleaned, original, is_valid, reason in records:
        if is_valid:
            gid = id_to_group.get(rid, rid)
            pri = id_is_primary.get(rid, 1)
        else:
            # Garbage rows: each gets own group, not primary, is_valid=0
            gid = rid
            pri = 0

        batch_data.append((cleaned, original, rid, gid, pri, is_valid, reason))

        if len(batch_data) >= BATCH:
            cur.executemany(insert_sql, batch_data)
            conn.commit()
            batch_data = []

    if batch_data:
        cur.executemany(insert_sql, batch_data)
        conn.commit()

    print(f"  Inserted {len(records)} rows.\n")

    # ── 9. Verify & stats ────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM aggressive_filter")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM aggressive_filter WHERE is_valid = 1")
    valid = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM aggressive_filter WHERE is_valid = 0")
    invalid = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM aggressive_filter WHERE is_primary = 1 AND is_valid = 1")
    primaries = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT group_id) FROM aggressive_filter WHERE is_valid = 1")
    unique_groups = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM masters WHERE type = 'Company'")
    master_total = cur.fetchone()[0]

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  masters Company rows:         {master_total}")
    print(f"  aggressive_filter total:       {total}")
    print(f"  Row count match:               {'YES' if total == master_total else 'NO'}")
    print(f"  Valid companies:               {valid}")
    print(f"  Garbage (is_valid=0):          {invalid}")
    print(f"  Unique groups (valid only):    {unique_groups}")
    print(f"  Primary records:               {primaries}")
    print(f"  Duplicates removed:            {valid - primaries}")
    print(f"  Dedup rate:                    {(valid - primaries) / valid * 100:.1f}%")

    # ── 10. Garbage breakdown ────────────────────────────────────
    cur.execute("""
        SELECT filter_reason, COUNT(*) 
        FROM aggressive_filter 
        WHERE is_valid = 0 
        GROUP BY filter_reason 
        ORDER BY COUNT(*) DESC
    """)
    print(f"\n  Garbage breakdown:")
    for reason, cnt in cur.fetchall():
        print(f"    {reason}: {cnt}")

    # ── 11. Verification: "reliance" keyword ─────────────────────
    keyword = "reliance"
    cur.execute(
        "SELECT COUNT(*) FROM masters WHERE type='Company' AND label LIKE %s",
        (f"%{keyword}%",),
    )
    m_hits = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM aggressive_filter WHERE company_name LIKE %s OR original_name LIKE %s",
        (f"%{keyword}%", f"%{keyword}%"),
    )
    af_hits = cur.fetchone()[0]

    print(f"\n  Verification '{keyword}': masters={m_hits}, aggressive_filter={af_hits}, match={'YES' if m_hits == af_hits else 'NO'}")

    # ── 12. Sample output ────────────────────────────────────────
    print(f"\n  Sample groups for '{keyword}':")
    cur.execute("""
        SELECT group_id, master_id, company_name, is_primary, is_valid
        FROM aggressive_filter
        WHERE (company_name LIKE %s OR original_name LIKE %s) AND is_valid = 1
        ORDER BY group_id, is_primary DESC
        LIMIT 40
    """, (f"%{keyword}%", f"%{keyword}%"))

    current_group = None
    for gid, mid, name, pri, val in cur.fetchall():
        if gid != current_group:
            current_group = gid
            print(f"\n    --- Group {gid} ---")
        marker = " ★ PRIMARY" if pri else ""
        print(f"      ID={mid:>6}  {name[:70]}{marker}")

    conn.close()
    elapsed = time.time() - t0
    print(f"\n=== Done in {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()

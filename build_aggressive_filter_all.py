#!/usr/bin/env python3
"""
Build the `aggressive_filter_all` table — same as aggressive_filter but
includes ALL rows from masters (Company, Archived, Group, etc.).

Key addition: Archived rows are matched against Company groups so that
searching 'reliance' in this table returns ALL 1001+ rows (same as masters),
with proper group assignments.

Columns:
    id              - auto-increment PK
    company_name    - CLEANED name
    original_name   - original label from masters
    master_id       - record ID in masters
    source_type     - original type from masters (Company, Archived, Group, etc.)
    group_id        - shared by all rows in the same duplicate group
    is_primary      - 1 = canonical row for the group, 0 = duplicate
    is_valid        - 1 = real entity, 0 = garbage
    filter_reason   - why flagged invalid (NULL if valid)

Usage:
    python3 build_aggressive_filter_all.py
"""

import os
import re
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

def clean_name(label):
    if not label:
        return ""
    s = label.strip()
    s = re.sub(r'\s*\xc2?\xb7\s*$', '', s)
    s = re.sub(r'^\s*\xc2?\xb7\s*', '', s)
    s = re.sub(r'^[\s,.\-\'""\[\]()·`=/#]+', '', s)
    s = re.sub(r'[\s,.\-\'""\[\]()·`=/#]+$', '', s)
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'(\w)\s+&(\w)', r'\1&\2', s)
    return s.strip()


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


def classify_garbage(cleaned):
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


# ─── Normalisation ───────────────────────────────────────────────

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


def normalize(label):
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


def token_signature(normalized):
    tokens = [t for t in normalized.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(sorted(tokens))


def composite_score(a, b):
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

def primary_score(label):
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
    print("=== Building aggressive_filter_all table ===\n")

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── 1. Create table ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aggressive_filter_all (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            company_name    VARCHAR(255) NOT NULL,
            original_name   VARCHAR(255),
            master_id       INT NOT NULL,
            source_type     VARCHAR(50) NOT NULL DEFAULT 'Company',
            group_id        INT NOT NULL,
            is_primary      TINYINT(1) NOT NULL DEFAULT 0,
            is_valid        TINYINT(1) NOT NULL DEFAULT 1,
            filter_reason   VARCHAR(100) NULL,
            INDEX idx_company_name (company_name(100)),
            INDEX idx_group_id (group_id),
            INDEX idx_master_id (master_id),
            INDEX idx_source_type (source_type),
            INDEX idx_is_primary (is_primary),
            INDEX idx_is_valid (is_valid)
        )
    """)
    cur.execute("DELETE FROM aggressive_filter_all")
    conn.commit()
    print("Table ready.\n")

    # ── 2. Load ALL rows from masters ────────────────────────────
    print("Loading ALL records from masters...")
    cur.execute("SELECT id, label, type FROM masters")
    all_rows = cur.fetchall()
    print(f"  Loaded {len(all_rows)} total rows.\n")

    # Separate by type
    company_rows = [(r[0], r[1], r[2]) for r in all_rows if r[2] == 'Company']
    other_rows = [(r[0], r[1], r[2]) for r in all_rows if r[2] != 'Company']
    print(f"  Company: {len(company_rows)}")
    print(f"  Other (Archived/Group/etc): {len(other_rows)}\n")

    # ── 3. Clean names & classify garbage (ALL rows) ─────────────
    print("Cleaning names & classifying garbage...")
    all_records = []  # (master_id, cleaned, original, source_type, is_valid, reason)
    garbage_count = 0
    for rid, label, rtype in all_rows:
        original = label or ""
        cleaned = clean_name(original)
        reason = classify_garbage(cleaned)
        is_valid = 0 if reason else 1
        if reason:
            garbage_count += 1
        all_records.append((rid, cleaned, original, rtype or 'Unknown', is_valid, reason))

    print(f"  Valid: {len(all_records) - garbage_count}")
    print(f"  Garbage (is_valid=0): {garbage_count}\n")

    # ── 4. Build lookup for ALL valid records ────────────────────
    valid_records = [(r[0], r[1]) for r in all_records if r[4] == 1 and r[1]]
    id_to_cleaned = {r[0]: r[1] for r in valid_records}
    id_to_norm = {rid: normalize(cleaned) for rid, cleaned in valid_records}

    # ── 5. Blocking + scoring on cleaned names (ALL types) ───────
    print("Blocking on cleaned names (all types together)...")
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

    # ── 6. Score & merge ─────────────────────────────────────────
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
    # Prefer Company rows as primary over Archived rows
    print("Building groups and selecting primaries...")
    groups = uf.groups()

    # Build type lookup
    id_to_type = {r[0]: r[3] for r in all_records}

    id_to_group = {}
    id_is_primary = {}

    TYPE_PRIORITY = {'Company': 10, 'Companny': 9, 'Group': 5, 'Archived': 1}

    for root, members in groups.items():
        scored_members = []
        for mid in members:
            label = id_to_cleaned.get(mid, "")
            ps = primary_score(label)
            # Boost Company types so they get picked as primary
            tp = TYPE_PRIORITY.get(id_to_type.get(mid, ''), 0)
            scored_members.append((mid, label, ps + tp * 10))

        scored_members.sort(key=lambda x: x[2], reverse=True)
        best_id = scored_members[0][0]

        for mid, _, _ in scored_members:
            id_to_group[mid] = best_id
            id_is_primary[mid] = 1 if mid == best_id else 0

    # Singletons
    for rid in id_to_cleaned:
        if rid not in id_to_group:
            id_to_group[rid] = rid
            id_is_primary[rid] = 1

    multi_groups = sum(1 for _, m in groups.items() if len(m) > 1)
    print(f"  Multi-member groups: {multi_groups}")
    print(f"  Singletons: {len(id_to_cleaned) - sum(len(m) for m in groups.values())}\n")

    # ── 8. Insert into table ─────────────────────────────────────
    print("Inserting into aggressive_filter_all...")
    BATCH = 5000
    insert_sql = (
        "INSERT INTO aggressive_filter_all "
        "(company_name, original_name, master_id, source_type, group_id, is_primary, is_valid, filter_reason) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )

    batch_data = []
    for rid, cleaned, original, src_type, is_valid, reason in all_records:
        if is_valid:
            gid = id_to_group.get(rid, rid)
            pri = id_is_primary.get(rid, 1)
        else:
            gid = rid
            pri = 0

        batch_data.append((cleaned, original, rid, src_type, gid, pri, is_valid, reason))

        if len(batch_data) >= BATCH:
            cur.executemany(insert_sql, batch_data)
            conn.commit()
            batch_data = []

    if batch_data:
        cur.executemany(insert_sql, batch_data)
        conn.commit()

    print(f"  Inserted {len(all_records)} rows.\n")

    # ── 9. Stats ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM aggressive_filter_all")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM masters")
    master_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aggressive_filter_all WHERE is_valid = 1")
    valid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aggressive_filter_all WHERE is_valid = 0")
    invalid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aggressive_filter_all WHERE is_primary = 1 AND is_valid = 1")
    primaries = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT group_id) FROM aggressive_filter_all WHERE is_valid = 1")
    unique_groups = cur.fetchone()[0]

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  masters total rows:            {master_total}")
    print(f"  aggressive_filter_all total:    {total}")
    print(f"  Row count match:               {'YES' if total == master_total else 'NO — gap: ' + str(master_total - total)}")
    print(f"  Valid:                          {valid}")
    print(f"  Garbage (is_valid=0):           {invalid}")
    print(f"  Unique groups (valid only):     {unique_groups}")
    print(f"  Primary records:                {primaries}")
    print(f"  Duplicates removed:             {valid - primaries}")
    print(f"  Dedup rate:                     {(valid - primaries) / valid * 100:.1f}%")

    # Type breakdown
    cur.execute("""
        SELECT source_type, COUNT(*), 
               SUM(CASE WHEN is_valid=1 THEN 1 ELSE 0 END) as valid_ct,
               SUM(CASE WHEN is_valid=0 THEN 1 ELSE 0 END) as invalid_ct
        FROM aggressive_filter_all 
        GROUP BY source_type
        ORDER BY COUNT(*) DESC
    """)
    print(f"\n  By source_type:")
    for stype, cnt, v, iv in cur.fetchall():
        print(f"    {stype}: {cnt} total ({v} valid, {iv} garbage)")

    # Garbage breakdown
    cur.execute("""
        SELECT filter_reason, COUNT(*) FROM aggressive_filter_all 
        WHERE is_valid = 0 GROUP BY filter_reason ORDER BY COUNT(*) DESC
    """)
    print(f"\n  Garbage breakdown:")
    for reason, cnt in cur.fetchall():
        print(f"    {reason}: {cnt}")

    # ── 10. Verification: "reliance" ─────────────────────────────
    keyword = "reliance"
    cur.execute(
        "SELECT COUNT(*) FROM masters WHERE label LIKE %s",
        (f"%{keyword}%",),
    )
    m_hits = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM aggressive_filter_all WHERE company_name LIKE %s OR original_name LIKE %s",
        (f"%{keyword}%", f"%{keyword}%"),
    )
    af_hits = cur.fetchone()[0]

    print(f"\n  Verification '{keyword}':")
    print(f"    masters (ALL types):              {m_hits}")
    print(f"    aggressive_filter_all:            {af_hits}")
    print(f"    Match: {'YES' if m_hits == af_hits else 'NO'}")

    # By source_type for reliance
    cur.execute("""
        SELECT source_type, COUNT(*) FROM aggressive_filter_all 
        WHERE company_name LIKE %s OR original_name LIKE %s
        GROUP BY source_type ORDER BY COUNT(*) DESC
    """, (f"%{keyword}%", f"%{keyword}%"))
    print(f"    Breakdown:")
    for st, c in cur.fetchall():
        print(f"      {st}: {c}")

    # Sample
    print(f"\n  Sample groups for '{keyword}' (showing cross-type groups):")
    cur.execute("""
        SELECT group_id, master_id, company_name, source_type, is_primary, is_valid
        FROM aggressive_filter_all
        WHERE (company_name LIKE %s OR original_name LIKE %s) AND is_valid = 1
        ORDER BY group_id, is_primary DESC, source_type
        LIMIT 40
    """, (f"%{keyword}%", f"%{keyword}%"))

    current_group = None
    for gid, mid, name, stype, pri, val in cur.fetchall():
        if gid != current_group:
            current_group = gid
            print(f"\n    --- Group {gid} ---")
        marker = " ★ PRIMARY" if pri else ""
        print(f"      [{stype:>10}] ID={mid:>6}  {name[:60]}{marker}")

    conn.close()
    elapsed = time.time() - t0
    print(f"\n=== Done in {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()

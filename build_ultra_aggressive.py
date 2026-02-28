#!/usr/bin/env python3
"""
Build `ultra_aggressive_filter` — maximum dedup with deep cleaning.

Improvements over aggressive_filter_all:
  1. DEEP NAME CLEANING:
     - Strip LinkedIn artifacts: "· Full-time", "· Part-time", "·", etc.
     - Strip trailing locations: ", Mumbai", ", India", ", New Delhi", etc.
     - Remove parenthetical content: "(formerly XYZ)", "(A Tata Enterprise)"
     - Strip trailing unclosed parens: "Reliance (ADAG"
     - Remove leading bullets/dots: "•Reliance" → "Reliance"
     - Collapse all spacing issues

  2. DEEPER GARBAGE FILTERING:
     - Job titles as company names (Consultant, Advisor, Manager, etc.)
     - Generic descriptions ("working for", "currently at", etc.)
     - Single-word generic names (< 4 chars after cleaning)

  3. MORE AGGRESSIVE MATCHING:
     - Lower threshold: 0.60 (vs 0.70)
     - Larger block sizes: 1000 (vs 500)
     - Additional blocking: first 4 chars + first 5 chars
     - Phonetic-style: sorted bigrams blocking

  4. ALL TYPES included (Company + Archived + Group + Companny)

Usage:
    python3 build_ultra_aggressive.py
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

# ─── Deep cleaning ───────────────────────────────────────────────

# LinkedIn job-type artifacts
_LINKEDIN_SUFFIX_RE = re.compile(
    r'\s*·\s*(?:Full-time|Part-time|Contract|Internship|Seasonal|Temporary|Freelance|Self-employed|Apprenticeship)?\s*$',
    re.IGNORECASE,
)

# Leading bullets/special chars
_LEADING_JUNK_RE = re.compile(r'^[\s•·\-–—,.\'""\[\](){}*/\\#`=:;!?|@&+]+')

# Trailing junk
_TRAILING_JUNK_RE = re.compile(r'[\s,.\-–—\'""\[\](){}*/\\#`=:;!?|@&+·•]+$')

# Parenthetical content (including unclosed parens)
_PAREN_RE = re.compile(r'\s*\([^)]*\)?\s*')

# Trailing locations — common Indian and global cities/countries
_LOCATIONS = (
    'India', 'USA', 'UK', 'UAE', 'Singapore', 'Dubai', 'London', 'Germany',
    'Australia', 'Canada', 'Japan', 'China', 'Africa', 'Europe', 'Asia',
    'Mumbai', 'Delhi', 'New Delhi', 'Delhi/NCR', 'NCR', 'Bangalore', 'Bengaluru',
    'Chennai', 'Pune', 'Hyderabad', 'Kolkata', 'Ahmedabad', 'Jaipur',
    'Noida', 'Gurgaon', 'Gurugram', 'Ghaziabad', 'Lucknow', 'Chandigarh',
    'Indore', 'Bhopal', 'Nagpur', 'Vadodara', 'Surat', 'Kochi', 'Cochin',
    'Thiruvananthapuram', 'Coimbatore', 'Visakhapatnam', 'Mysore', 'Mysuru',
    'Jamnagar', 'Rajkot', 'Patna', 'Ranchi', 'Bhubaneswar', 'Guwahati',
    'Dehradun', 'Shimla', 'Amritsar', 'Ludhiana', 'Jalandhar',
    'Siliguri', 'Jodhpur', 'Udaipur', 'Varanasi', 'Agra', 'Kanpur',
    'Meerut', 'Faridabad', 'Gwalior', 'Jabalpur', 'Aurangabad',
    'Nashik', 'Thane', 'Navi Mumbai', 'Mangalore',
    'Head Office', 'Corporate Office', 'Corporate', 'HQ',
)
_LOCATION_PATTERN = '|'.join(re.escape(loc) for loc in sorted(_LOCATIONS, key=len, reverse=True))
_TRAILING_LOCATION_RE = re.compile(
    r'[,\s]+(?:' + _LOCATION_PATTERN + r')\s*$',
    re.IGNORECASE,
)


def deep_clean(label):
    """Ultra-aggressive name cleaning."""
    if not label:
        return ""
    s = label.strip()

    # Strip LinkedIn artifacts first (before other cleaning)
    s = _LINKEDIN_SUFFIX_RE.sub('', s)

    # Remove parenthetical content
    s = _PAREN_RE.sub(' ', s)

    # Leading junk
    s = _LEADING_JUNK_RE.sub('', s)
    # Trailing junk
    s = _TRAILING_JUNK_RE.sub('', s)

    # Strip trailing locations (up to 2 passes for nested: "Ltd., Mumbai, India")
    for _ in range(3):
        prev = s
        s = _TRAILING_LOCATION_RE.sub('', s)
        s = _TRAILING_JUNK_RE.sub('', s)
        if s == prev:
            break

    # Collapse spaces
    s = re.sub(r'\s+', ' ', s)

    # Fix & spacing
    s = re.sub(r'(\w)\s+&(\w)', r'\1&\2', s)

    # Final trim
    s = s.strip()
    return s


# ─── Garbage detection (stricter) ────────────────────────────────

_PLACEHOLDER_RE = re.compile(
    r'^(test|testing|unknown|n/?a|none|null|na|tbd|temp|temporary|sample|'
    r'dummy|xxx|zzz|abc|asdf|qwerty|company\s*\d*|no\s*company|not\s*applicable|'
    r'confidential|private|personal|do\s*not\s*use|delete|removed|undefined|'
    r'current|present|same|same\s+as\s+above|as\s+above|ditto|above|below|'
    r'various|multiple|several|many|other|others|etc|misc|miscellaneous)$',
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

# Job titles used as company names
_JOB_TITLE_RE = re.compile(
    r'^(consultant|advisor|manager|director|engineer|analyst|'
    r'developer|designer|architect|teacher|professor|lecturer|'
    r'doctor|lawyer|advocate|attorney|accountant|auditor|'
    r'trainer|coach|mentor|tutor|instructor|'
    r'volunteer|intern|trainee|apprentice|'
    r'partner|founder|co-?\s*founder|entrepreneur|'
    r'ceo|cto|cfo|coo|cio|cmo|vp|svp|evp|md|gm)s?$',
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
    if _JOB_TITLE_RE.match(cleaned):
        return "job_title_not_company"
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
    r"\bindustries\b", r"\bservices?\b", r"\bsolutions?\b",
    r"\bglobal\b", r"\bsystems?\b", r"\bnetwork[s]?\b",
    r"\bmanagement\b", r"\bassociates?\b", r"\bpartners?\b",
    r"\bventures?\b", r"\bcapital\b", r"\bfoundation\b",
    r"\binstitute\b", r"\bacademy\b", r"\buniversity\b",
    r"\bhospital[s]?\b", r"\bclinic[s]?\b", r"\bpharmaceuticals?\b",
    r"\blaborator(?:y|ies)\b", r"\bresearch\b",
]
LEGAL_SUFFIX_RE = re.compile(
    r"(?:" + "|".join(LEGAL_SUFFIXES) + r")", re.IGNORECASE
)
STOPWORDS = {"the", "and", "of", "for", "in", "a", "an", "to", "at", "by", "on", "is", "it", "or"}


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

def primary_score(label, source_type='Company'):
    score = 0.0
    if not label or len(label.strip()) < 2:
        return -10
    # Prefer Company over Archived
    type_bonus = {'Company': 100, 'Companny': 90, 'Group': 50, 'Archived': 10}
    score += type_bonus.get(source_type, 0)

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
    print("=== Building ultra_aggressive_filter table ===\n")

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── 1. Create table ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ultra_aggressive_filter (
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
    cur.execute("DELETE FROM ultra_aggressive_filter")
    conn.commit()
    print("Table ready.\n")

    # ── 2. Load ALL rows ─────────────────────────────────────────
    print("Loading ALL records from masters...")
    cur.execute("SELECT id, label, type FROM masters")
    all_rows = cur.fetchall()
    print(f"  Loaded {len(all_rows)} total rows.\n")

    # ── 3. Deep clean & classify ─────────────────────────────────
    print("Deep cleaning & classifying...")
    all_records = []
    garbage_count = 0
    for rid, label, rtype in all_rows:
        original = label or ""
        cleaned = deep_clean(original)
        reason = classify_garbage(cleaned)
        is_valid = 0 if reason else 1
        if reason:
            garbage_count += 1
        all_records.append((rid, cleaned, original, rtype or 'Unknown', is_valid, reason))

    valid_count = len(all_records) - garbage_count
    print(f"  Valid: {valid_count}")
    print(f"  Garbage: {garbage_count}\n")

    # ── 4. Build lookup ──────────────────────────────────────────
    valid_records = [(r[0], r[1]) for r in all_records if r[4] == 1 and r[1]]
    id_to_cleaned = {r[0]: r[1] for r in valid_records}
    id_to_norm = {rid: normalize(cleaned) for rid, cleaned in valid_records}
    id_to_type = {r[0]: r[3] for r in all_records}

    # ── 5. Multi-strategy blocking ───────────────────────────────
    print("Multi-strategy blocking...")
    MAX_BLOCK = 1000  # bigger blocks

    block_prefix3 = defaultdict(set)
    block_prefix4 = defaultdict(set)
    block_prefix5 = defaultdict(set)
    block_token = defaultdict(set)

    for rid, norm in id_to_norm.items():
        if len(norm) >= 3:
            block_prefix3[norm[:3]].add(rid)
        if len(norm) >= 4:
            block_prefix4[norm[:4]].add(rid)
        if len(norm) >= 5:
            block_prefix5[norm[:5]].add(rid)
        tsig = token_signature(norm)
        if tsig and len(tsig.split()) > 1:
            block_token[tsig].add(rid)

    candidate_pairs = set()
    for bd in (block_prefix3, block_prefix4, block_prefix5, block_token):
        for key, members in bd.items():
            if len(members) < 2 or len(members) > MAX_BLOCK:
                continue
            ml = sorted(members)
            for i in range(len(ml)):
                for j in range(i + 1, len(ml)):
                    candidate_pairs.add((ml[i], ml[j]))

    print(f"  Candidate pairs: {len(candidate_pairs)}")

    # ── 6. Score & merge (ultra-aggressive) ──────────────────────
    THRESH = 0.60  # very aggressive
    print(f"Scoring pairs (threshold={THRESH})...")
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
        if sc >= THRESH:
            uf.union(id_a, id_b)
        scored += 1
        if scored % 2000000 == 0:
            print(f"    Scored {scored} pairs...")

    print(f"  Scored {scored} pairs total.\n")

    # ── 7. Groups & primaries ────────────────────────────────────
    print("Building groups...")
    groups = uf.groups()

    id_to_group = {}
    id_is_primary = {}

    for root, members in groups.items():
        scored_members = [
            (mid, id_to_cleaned.get(mid, ""),
             primary_score(id_to_cleaned.get(mid, ""), id_to_type.get(mid, 'Company')))
            for mid in members
        ]
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
    singletons = len(id_to_cleaned) - sum(len(m) for m in groups.values())
    print(f"  Singletons: {singletons}\n")

    # ── 8. Insert ────────────────────────────────────────────────
    print("Inserting...")
    BATCH = 5000
    insert_sql = (
        "INSERT INTO ultra_aggressive_filter "
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
    cur.execute("SELECT COUNT(*) FROM ultra_aggressive_filter")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM masters")
    master_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ultra_aggressive_filter WHERE is_valid=1")
    valid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ultra_aggressive_filter WHERE is_valid=0")
    invalid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ultra_aggressive_filter WHERE is_primary=1 AND is_valid=1")
    primaries = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT group_id) FROM ultra_aggressive_filter WHERE is_valid=1")
    unique_groups = cur.fetchone()[0]

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  masters total:                  {master_total}")
    print(f"  ultra_aggressive total:          {total}")
    print(f"  Row match:                       {'YES' if total == master_total else 'NO'}")
    print(f"  Valid:                           {valid}")
    print(f"  Garbage:                         {invalid}")
    print(f"  Unique groups (valid):           {unique_groups}")
    print(f"  Primaries:                       {primaries}")
    print(f"  Duplicates removed:              {valid - primaries}")
    print(f"  Dedup rate:                      {(valid - primaries) / valid * 100:.1f}%")

    # Comparison with previous tables
    cur.execute("SELECT COUNT(DISTINCT group_id) FROM final_result")
    fr_groups = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT group_id) FROM aggressive_filter_all WHERE is_valid=1")
    af_groups = cur.fetchone()[0]

    print(f"\n  COMPARISON:")
    print(f"    final_result groups:           {fr_groups}")
    print(f"    aggressive_filter_all groups:   {af_groups}")
    print(f"    ultra_aggressive groups:        {unique_groups}")
    print(f"    Improvement over final_result:  {fr_groups - unique_groups} fewer groups ({(fr_groups - unique_groups)/fr_groups*100:.1f}%)")

    # By type
    cur.execute("""
        SELECT source_type, COUNT(*),
               SUM(CASE WHEN is_valid=1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN is_valid=0 THEN 1 ELSE 0 END)
        FROM ultra_aggressive_filter GROUP BY source_type ORDER BY COUNT(*) DESC
    """)
    print(f"\n  By source_type:")
    for st, ct, v, iv in cur.fetchall():
        print(f"    {st}: {ct} ({v} valid, {iv} garbage)")

    # Garbage breakdown
    cur.execute("""
        SELECT filter_reason, COUNT(*) FROM ultra_aggressive_filter
        WHERE is_valid=0 GROUP BY filter_reason ORDER BY COUNT(*) DESC
    """)
    print(f"\n  Garbage breakdown:")
    for reason, cnt in cur.fetchall():
        print(f"    {reason}: {cnt}")

    # Reliance verification
    keyword = "reliance"
    cur.execute("SELECT COUNT(*) FROM masters WHERE label LIKE %s", (f"%{keyword}%",))
    m_hits = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM ultra_aggressive_filter WHERE company_name LIKE %s OR original_name LIKE %s",
        (f"%{keyword}%", f"%{keyword}%"),
    )
    u_hits = cur.fetchone()[0]
    print(f"\n  '{keyword}': masters={m_hits}, ultra={u_hits}, match={'YES' if m_hits == u_hits else 'NO'}")

    # Sample cross-type groups
    print(f"\n  Sample groups for '{keyword}':")
    cur.execute("""
        SELECT group_id, master_id, company_name, source_type, is_primary
        FROM ultra_aggressive_filter
        WHERE (company_name LIKE %s OR original_name LIKE %s) AND is_valid=1
        ORDER BY group_id, is_primary DESC
        LIMIT 50
    """, (f"%{keyword}%", f"%{keyword}%"))
    cg = None
    for gid, mid, name, st, pri in cur.fetchall():
        if gid != cg:
            cg = gid
            print(f"\n    --- Group {gid} ---")
        mk = " ★" if pri else ""
        print(f"      [{st:>10}] ID={mid:>6}  {name[:65]}{mk}")

    conn.close()
    print(f"\n=== Done in {time.time()-t0:.1f}s ===")


if __name__ == "__main__":
    main()

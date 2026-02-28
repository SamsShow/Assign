#!/usr/bin/env python3
"""
Build `improved_result` table — a balanced dedup table that sits between
`final_result` (too conservative) and `aggressive_filter` (over-groups).

KEY IMPROVEMENTS:
  1. INCLUDES ALL ROW TYPES: Company, Archived, Group, Companny — not just Company.
  2. SMART CLEANING:
     - Strips LinkedIn artifacts, junk chars, collapses whitespace.
     - Removes parenthetical content BEFORE matching (so "Titan Industries Ltd.,
       (Titan Automation Solutions)" → "Titan Industries Ltd" → groups correctly).
     - Strips trailing locations ("Mumbai", "India", etc.).
  3. SMART NORMALIZATION — only strips truly generic legal suffixes (Ltd, Inc,
     Corp, Pvt, LLC, etc.) and KEEPS differentiating words like "Industries",
     "Automation", "Technologies", "Consulting" that distinguish companies.
     This prevents "Titan Industries" from incorrectly merging with
     "Titan Automation Solutions".
  4. BALANCED THRESHOLD: 0.80 composite score (vs 0.70 in aggressive_filter,
     ~0.93 implied in final_result).  Catches more real duplicates without
     over-grouping.
  5. EXTRA SAFETY: after scoring, rejects merges where the core tokens
     (first 2 significant words) differ, preventing false positives.

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
    python3 build_improved_result.py
"""

import os
import re
import time
from collections import defaultdict

import mysql.connector
from rapidfuzz import fuzz
from dotenv import dotenv_values

cfg = dotenv_values(os.path.join(os.path.dirname(__file__), ".env"))

DB_CONFIG = {
    "host": cfg.get("DB_HOST"),
    "user": cfg.get("DB_USER"),
    "password": cfg.get("DB_PASSWORD"),
    "database": cfg.get("DB_NAME"),
    "connection_timeout": 120,
    "autocommit": False,
}


# ─── Name cleaning ───────────────────────────────────────────────

_LINKEDIN_SUFFIX_RE = re.compile(
    r"\s*·\s*(?:Full-time|Part-time|Contract|Internship|Seasonal|"
    r"Temporary|Freelance|Self-employed|Apprenticeship)?\s*$",
    re.IGNORECASE,
)

_LEADING_JUNK_RE = re.compile(r"^[\s•·\-–—,.\'\"\u201c\u201d\[\](){}*/\\#`=:;!?|@&+]+")
_TRAILING_JUNK_RE = re.compile(r"[\s,.\-–—\'\"\u201c\u201d\[\](){}*/\\#`=:;!?|@&+·•]+$")
_PAREN_RE = re.compile(r"\s*\([^)]*\)?\s*")

_LOCATIONS = (
    "India", "USA", "UK", "UAE", "Singapore", "Dubai", "London", "Germany",
    "Australia", "Canada", "Japan", "China", "Africa", "Europe", "Asia",
    "Mumbai", "Delhi", "New Delhi", "Delhi/NCR", "NCR", "Bangalore", "Bengaluru",
    "Chennai", "Pune", "Hyderabad", "Kolkata", "Ahmedabad", "Jaipur",
    "Noida", "Gurgaon", "Gurugram", "Ghaziabad", "Lucknow", "Chandigarh",
    "Indore", "Bhopal", "Nagpur", "Vadodara", "Surat", "Kochi", "Cochin",
    "Thiruvananthapuram", "Coimbatore", "Visakhapatnam", "Mysore", "Mysuru",
    "Jamnagar", "Rajkot", "Patna", "Ranchi", "Bhubaneswar", "Guwahati",
    "Dehradun", "Shimla", "Amritsar", "Ludhiana", "Jalandhar",
    "Siliguri", "Jodhpur", "Udaipur", "Varanasi", "Agra", "Kanpur",
    "Meerut", "Faridabad", "Gwalior", "Jabalpur", "Aurangabad",
    "Nashik", "Thane", "Navi Mumbai", "Mangalore",
    "Head Office", "Corporate Office", "Corporate", "HQ",
)
_LOCATION_PATTERN = "|".join(
    re.escape(loc) for loc in sorted(_LOCATIONS, key=len, reverse=True)
)
_TRAILING_LOCATION_RE = re.compile(
    r"[,\s]+(?:" + _LOCATION_PATTERN + r")\s*$",
    re.IGNORECASE,
)


def clean_name(label: str) -> str:
    """Clean a company name — moderate aggression."""
    if not label:
        return ""
    s = label.strip()

    # Strip LinkedIn artifacts
    s = _LINKEDIN_SUFFIX_RE.sub("", s)

    # Remove parenthetical content
    # This ensures "Titan Industries Ltd., (Titan Automation Solutions)"
    # → "Titan Industries Ltd" for proper grouping
    s = _PAREN_RE.sub(" ", s)

    # Leading/trailing junk
    s = _LEADING_JUNK_RE.sub("", s)
    s = _TRAILING_JUNK_RE.sub("", s)

    # Strip trailing locations (2 passes for "Ltd., Mumbai, India")
    for _ in range(3):
        prev = s
        s = _TRAILING_LOCATION_RE.sub("", s)
        s = _TRAILING_JUNK_RE.sub("", s)
        if s == prev:
            break

    # Collapse spaces
    s = re.sub(r"\s+", " ", s)

    # Fix & spacing: "AT &T" → "AT&T"
    s = re.sub(r"(\w)\s+&(\w)", r"\1&\2", s)

    return s.strip()


# ─── Garbage detection ───────────────────────────────────────────

_PLACEHOLDER_RE = re.compile(
    r"^(test|testing|unknown|n/?a|none|null|na|tbd|temp|temporary|sample|"
    r"dummy|xxx|zzz|abc|asdf|qwerty|company\s*\d*|no\s*company|not\s*applicable|"
    r"confidential|private|personal|do\s*not\s*use|delete|removed|undefined|"
    r"current|present|same|same\s+as\s+above|as\s+above|ditto|above|below|"
    r"various|multiple|several|many|other|others|etc|misc|miscellaneous)$",
    re.IGNORECASE,
)
_NON_COMPANY_RE = re.compile(
    r"(?:^|\s|·\s*)(freelanc\w*|self[\s-]?employ\w*|unemploy\w*|"
    r"retired|retirement|student|homemaker|housewife|"
    r"looking\s+for\s+(?:job|work|opportunit|a\s+job)|"
    r"job\s+seek\w*|between\s+jobs|career\s+break|"
    r"open\s+to\s+work|actively\s+seeking)",
    re.IGNORECASE,
)
_JOB_TITLE_RE = re.compile(
    r"^(consultant|advisor|manager|director|engineer|analyst|"
    r"developer|designer|architect|teacher|professor|lecturer|"
    r"doctor|lawyer|advocate|attorney|accountant|auditor|"
    r"trainer|coach|mentor|tutor|instructor|"
    r"volunteer|intern|trainee|apprentice|"
    r"partner|founder|co-?\s*founder|entrepreneur|"
    r"ceo|cto|cfo|coo|cio|cmo|vp|svp|evp|md|gm)s?$",
    re.IGNORECASE,
)
_SPECIAL_ONLY_RE = re.compile(r"^[^a-zA-Z0-9\u00C0-\u024F\u0400-\u04FF]+$")
_NUMERIC_ONLY_RE = re.compile(r"^[0-9]+$")


def classify_garbage(cleaned: str) -> str | None:
    """Return reason string if garbage/non-company, else None."""
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


# ─── SMART normalisation (key difference from aggressive_filter) ─

# ONLY strip truly generic legal entity suffixes.
# DO NOT strip differentiating words like "industries", "technologies",
# "consulting", "solutions", "services", "automation", etc.
# Those words distinguish companies (Titan Industries ≠ Titan Automation).
LEGAL_SUFFIXES_MINIMAL = [
    r"\bincorporated\b",
    r"\binc\b\.?",
    r"\bllc\b\.?",
    r"\bl\.l\.c\.?",
    r"\blimited\b",
    r"\bltd\b\.?",
    r"\bcorporation\b",
    r"\bcorp\b\.?",
    r"\bco\b\.?",
    r"\bplc\b\.?",
    r"\bgmbh\b",
    r"\bag\b",
    r"\bs\.?a\.?\b",
    r"\bn\.?v\.?\b",
    r"\bpvt\b\.?",
    r"\bprivate\b",
    r"\bl\.?p\.?\b",
    r"\bllp\b\.?",
]
LEGAL_SUFFIX_RE = re.compile(
    r"(?:" + "|".join(LEGAL_SUFFIXES_MINIMAL) + r")", re.IGNORECASE
)

STOPWORDS = {"the", "and", "of", "for", "in", "a", "an", "to", "at", "by", "on"}


def normalize(label: str) -> str:
    """
    Normalise for comparison — keeps differentiating business words intact.
    Only strips generic legal suffixes (Ltd, Inc, Corp, Pvt, LLC, etc.).
    """
    if not label:
        return ""
    s = label.lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Strip only truly generic suffixes
    s = LEGAL_SUFFIX_RE.sub("", s)
    s = re.sub(r"^the\s+", "", s)
    s = re.sub(r"\s+the$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def token_signature(normalized: str) -> str:
    """Sorted non-stopword tokens for blocking."""
    tokens = [t for t in normalized.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(sorted(tokens))


def get_core_tokens(normalized: str) -> list:
    """
    Extract the first N significant (non-stopword, len>1) tokens.
    Used to verify that two companies share the same core identity.
    """
    tokens = [t for t in normalized.split() if t not in STOPWORDS and len(t) > 1]
    return tokens[:2] if len(tokens) >= 2 else tokens


def composite_score(a: str, b: str) -> float:
    """Weighted fuzzy composite of 4 metrics."""
    if not a or not b:
        return 0.0
    tsr = fuzz.token_sort_ratio(a, b) / 100.0
    tse = fuzz.token_set_ratio(a, b) / 100.0
    rat = fuzz.ratio(a, b) / 100.0
    par = fuzz.partial_ratio(a, b) / 100.0
    # Slightly reduce partial_ratio weight to prevent short-string false matches
    return 0.30 * tsr + 0.30 * tse + 0.25 * rat + 0.15 * par


def should_merge(norm_a: str, norm_b: str, score: float) -> bool:
    """
    Extra safety check: after scoring, verify the core tokens overlap.
    This prevents merging "Titan Industries" with "Titan Automation Solutions"
    even if partial_ratio is high because they share "Titan".
    """
    if score < 0.80:
        return False

    # If exact normalised match, always merge
    if norm_a == norm_b:
        return True

    # High confidence — merge without additional checks
    if score >= 0.92:
        return True

    # Medium confidence (0.80–0.92): verify core tokens overlap
    tokens_a = get_core_tokens(norm_a)
    tokens_b = get_core_tokens(norm_b)

    if not tokens_a or not tokens_b:
        return score >= 0.85

    # Check if core tokens overlap sufficiently
    # "titan industries" vs "titan industry" → cores overlap via fuzzy
    # "titan industries" vs "titan automation" → cores differ
    core_a = set(tokens_a)
    core_b = set(tokens_b)

    # Direct overlap
    if core_a == core_b:
        return True

    # Fuzzy core match: at least one core token pair must be very similar
    matches = 0
    for ta in core_a:
        for tb in core_b:
            if fuzz.ratio(ta, tb) >= 75:  # "industries" vs "industry" → ~78
                matches += 1
                break

    # Require at least half of the smaller set's tokens to match
    min_core = min(len(core_a), len(core_b))
    return matches >= min_core


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


def primary_score(label: str, source_type: str = "Company") -> float:
    """Score a label for primary selection.  Prefer Company type, proper case."""
    score = 0.0
    if not label or len(label.strip()) < 2:
        return -10

    # Strongly prefer Company type as primary
    type_bonus = {"Company": 100, "Companny": 90, "Group": 50, "Archived": 10}
    score += type_bonus.get(source_type, 0)

    if label == label.upper():
        score -= 1  # ALL CAPS is less canonical
    elif label == label.lower():
        score -= 1
    elif label[0].isupper():
        score += 2  # Title case preferred

    if _LEGAL_SIMPLE.search(label):
        score += 1  # Has a legal suffix → more complete name

    length = len(label.strip())
    if length >= 5:
        score += min(length / 30.0, 1.0)

    if "  " in label or label != label.strip():
        score -= 1  # Messy whitespace

    return score


# ─── Main ────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=" * 60)
    print("Building `improved_result` table")
    print("  Aggression: moderately above final_result")
    print("  Includes: ALL types (Company + Archived + Group + ...)")
    print("  Threshold: 0.80 composite + core-token safety check")
    print("=" * 60 + "\n")

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── 1. Create table ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS improved_result (
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
    cur.execute("DELETE FROM improved_result")
    conn.commit()
    print("Table `improved_result` ready.\n")

    # ── 2. Load ALL rows from masters ────────────────────────────
    print("Loading ALL records from masters...")
    cur.execute("SELECT id, label, type FROM masters")
    all_rows = cur.fetchall()
    print(f"  Loaded {len(all_rows)} total rows.\n")

    # ── 3. Clean & classify ──────────────────────────────────────
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
        all_records.append((rid, cleaned, original, rtype or "Unknown", is_valid, reason))

    valid_count = len(all_records) - garbage_count
    print(f"  Valid: {valid_count}")
    print(f"  Garbage (is_valid=0): {garbage_count}\n")

    # ── 4. Build lookups for valid records ───────────────────────
    valid_records = [(r[0], r[1]) for r in all_records if r[4] == 1 and r[1]]
    id_to_cleaned = {r[0]: r[1] for r in valid_records}
    id_to_norm = {rid: normalize(cleaned) for rid, cleaned in valid_records}
    id_to_type = {r[0]: r[3] for r in all_records}

    # ── 5. Blocking ──────────────────────────────────────────────
    print("Building candidate pairs via blocking...")
    MAX_BLOCK = 500

    block_prefix3 = defaultdict(set)
    block_prefix4 = defaultdict(set)
    block_token = defaultdict(set)
    block_first_word = defaultdict(set)
    block_word_prefix = defaultdict(set)

    block_compound = defaultdict(set)  # first_word + second_word[:3]

    for rid, norm in id_to_norm.items():
        if len(norm) >= 3:
            block_prefix3[norm[:3]].add(rid)
        if len(norm) >= 4:
            block_prefix4[norm[:4]].add(rid)
        tsig = token_signature(norm)
        if tsig and len(tsig.split()) > 1:
            block_token[tsig].add(rid)
        # First word blocking: groups all "titan ..." together
        words = [w for w in norm.split() if w not in STOPWORDS and len(w) > 2]
        if words:
            block_first_word[words[0]].add(rid)
        # Word-prefix blocking: for each word >4 chars, use word[:5]
        for w in words:
            if len(w) > 4:
                block_word_prefix[w[:5]].add(rid)
        # Compound blocking: first_word + second_word[:3]
        # Handles high-frequency names like "reliance ind..." where
        # first_word alone exceeds MAX_BLOCK
        if len(words) >= 2:
            compound_key = words[0] + "_" + words[1][:3]
            block_compound[compound_key].add(rid)

    candidate_pairs = set()
    for bd in (block_prefix3, block_prefix4, block_token,
               block_first_word, block_word_prefix, block_compound):
        for key, members in bd.items():
            if len(members) < 2 or len(members) > MAX_BLOCK:
                continue
            ml = sorted(members)
            for i in range(len(ml)):
                for j in range(i + 1, len(ml)):
                    candidate_pairs.add((ml[i], ml[j]))

    print(f"  Candidate pairs: {len(candidate_pairs):,}\n")

    # ── 6. Score & merge with safety checks ──────────────────────
    print("Scoring pairs (threshold=0.80 + core-token check)...")
    uf = UnionFind()
    scored = 0
    merged = 0

    for id_a, id_b in candidate_pairs:
        na = id_to_norm.get(id_a, "")
        nb = id_to_norm.get(id_b, "")
        if not na or not nb:
            continue

        # Exact normalised match → always merge
        if na == nb:
            uf.union(id_a, id_b)
            scored += 1
            merged += 1
            continue

        sc = composite_score(na, nb)
        if should_merge(na, nb, sc):
            uf.union(id_a, id_b)
            merged += 1

        scored += 1
        if scored % 2_000_000 == 0:
            print(f"    Scored {scored:,} pairs...")

    print(f"  Scored {scored:,} pairs, merged {merged:,}.\n")

    # ── 7. Build groups & select primaries ───────────────────────
    print("Building groups & selecting primaries...")
    groups = uf.groups()

    id_to_group = {}
    id_is_primary = {}

    for root, members in groups.items():
        scored_members = [
            (
                mid,
                id_to_cleaned.get(mid, ""),
                primary_score(
                    id_to_cleaned.get(mid, ""),
                    id_to_type.get(mid, "Company"),
                ),
            )
            for mid in members
        ]
        scored_members.sort(key=lambda x: x[2], reverse=True)
        best_id = scored_members[0][0]

        for mid, _, _ in scored_members:
            id_to_group[mid] = best_id
            id_is_primary[mid] = 1 if mid == best_id else 0

    # Singletons: valid records not in any union-find group
    for rid in id_to_cleaned:
        if rid not in id_to_group:
            id_to_group[rid] = rid
            id_is_primary[rid] = 1

    multi_groups = sum(1 for _, m in groups.items() if len(m) > 1)
    grouped_ids = set()
    for members in groups.values():
        grouped_ids.update(members)
    singleton_count = sum(1 for rid in id_to_cleaned if rid not in grouped_ids)
    print(f"  Multi-member groups: {multi_groups:,}")
    print(f"  Singletons: {singleton_count:,}\n")

    # ── 8. Insert into table ─────────────────────────────────────
    # Reconnect — the connection may have timed out during scoring
    try:
        cur.close()
        conn.close()
    except Exception:
        pass
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Re-clear in case partial data from a prior attempt remains
    cur.execute("DELETE FROM improved_result")
    conn.commit()

    print("Inserting into `improved_result`...")
    BATCH = 5000
    insert_sql = (
        "INSERT INTO improved_result "
        "(company_name, original_name, master_id, source_type, "
        "group_id, is_primary, is_valid, filter_reason) "
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

    print(f"  Inserted {len(all_records):,} rows.\n")

    # ── 9. Summary stats ─────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM improved_result")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM masters")
    master_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM improved_result WHERE is_valid=1")
    valid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM improved_result WHERE is_valid=0")
    invalid = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM improved_result WHERE is_primary=1 AND is_valid=1"
    )
    primaries = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(DISTINCT group_id) FROM improved_result WHERE is_valid=1"
    )
    unique_groups = cur.fetchone()[0]

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  masters total:                   {master_total:,}")
    print(f"  improved_result total:           {total:,}")
    print(f"  Row match:                       {'YES' if total == master_total else 'NO'}")
    print(f"  Valid:                           {valid:,}")
    print(f"  Garbage:                         {invalid:,}")
    print(f"  Unique groups (valid):           {unique_groups:,}")
    print(f"  Primaries:                       {primaries:,}")
    print(f"  Duplicates removed:              {valid - primaries:,}")
    if valid > 0:
        print(f"  Dedup rate:                      {(valid - primaries) / valid * 100:.1f}%")

    # Comparison with other tables
    for tbl in ("final_result", "aggressive_filter", "aggressive_filter_all", "ultra_aggressive_filter"):
        try:
            cur.execute(f"SELECT COUNT(DISTINCT group_id) FROM {tbl} WHERE is_primary=1 OR is_primary=1")
            other_groups = cur.fetchone()[0]
            print(f"  {tbl} groups: {other_groups:,}")
        except Exception:
            try:
                cur.execute(f"SELECT COUNT(DISTINCT group_id) FROM {tbl}")
                other_groups = cur.fetchone()[0]
                print(f"  {tbl} groups: {other_groups:,}")
            except Exception:
                pass

    # By type
    cur.execute("""
        SELECT source_type, COUNT(*),
               SUM(CASE WHEN is_valid=1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN is_valid=0 THEN 1 ELSE 0 END)
        FROM improved_result GROUP BY source_type ORDER BY COUNT(*) DESC
    """)
    print(f"\n  By source_type:")
    for st, ct, v, iv in cur.fetchall():
        print(f"    {st}: {ct:,} ({v:,} valid, {iv:,} garbage)")

    # Garbage breakdown
    cur.execute("""
        SELECT filter_reason, COUNT(*) FROM improved_result
        WHERE is_valid=0 GROUP BY filter_reason ORDER BY COUNT(*) DESC
    """)
    print(f"\n  Garbage breakdown:")
    for reason, cnt in cur.fetchall():
        print(f"    {reason}: {cnt:,}")

    # ── 10. Verification: Titan ──────────────────────────────────
    for keyword in ("titan", "reliance"):
        cur.execute(
            "SELECT COUNT(*) FROM masters WHERE label LIKE %s",
            (f"%{keyword}%",),
        )
        m_hits = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM improved_result WHERE company_name LIKE %s OR original_name LIKE %s",
            (f"%{keyword}%", f"%{keyword}%"),
        )
        ir_hits = cur.fetchone()[0]
        print(f"\n  '{keyword}': masters={m_hits}, improved={ir_hits}, match={'YES' if m_hits == ir_hits else 'NO'}")

        print(f"  Sample groups for '{keyword}':")
        cur.execute("""
            SELECT group_id, master_id, company_name, source_type, is_primary
            FROM improved_result
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

    # ── 11. Useful queries ───────────────────────────────────────
    print(f"\n{'='*60}")
    print("USEFUL QUERIES")
    print(f"{'='*60}")
    print("  -- All rows for a keyword:")
    print("  SELECT * FROM improved_result WHERE company_name LIKE '%titan%';")
    print()
    print("  -- Deduplicated view:")
    print("  SELECT * FROM improved_result WHERE is_primary=1 AND is_valid=1;")
    print()
    print("  -- All members of a group:")
    print("  SELECT * FROM improved_result WHERE group_id=<id> ORDER BY is_primary DESC;")
    print()
    print("  -- Count unique companies:")
    print("  SELECT COUNT(DISTINCT group_id) FROM improved_result WHERE is_valid=1;")

    conn.close()
    print(f"\n=== Done in {time.time() - t0:.1f}s ===")


if __name__ == "__main__":
    main()

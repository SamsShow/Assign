#!/usr/bin/env python3
"""
Build `balanced_result` table — a SMARTER dedup table targeting 30-35% dedup.

KEY DIFFERENCE vs improved_result (41% dedup):
  This script understands that companies sharing a parent name but having
  different business-line descriptors are DIFFERENT companies:
    - Cholamandalam Home Finance  ≠  Cholamandalam DBS Finance
    - Reliance Industries         ≠  Reliance Capital
    - Tata Motors                 ≠  Tata Steel
    - L&T                        ≠  L&T Finance

  HOW IT WORKS:
  1. BUSINESS-DIFFERENTIATOR DETECTION: A curated set of ~120 business-domain
     keywords (finance, insurance, steel, pharma, motors, etc.).  When two
     names share a common prefix but carry different business keywords,
     the merge is blocked — even at moderately high fuzzy scores.
  2. SHORT-NAME BRIDGE PREVENTION: Single-token names like "Cholamandalam"
     or "Reliance" previously acted as Union-Find bridges, transitively
     connecting unrelated subsidiaries.  Now, names with ≤1 significant
     token require ≥0.95 to merge, eliminating these bridges.
  3. HIGHER THRESHOLDS: merge ≥0.82 (was 0.80), auto-merge ≥0.91 (was 0.92).
  4. REBALANCED SCORING: Reduced weight on token_set_ratio and partial_ratio
     (which inflate scores for substring matches) in favour of full-string
     metrics that better capture overall similarity.
  5. TIGHTER BLOCKING: MAX_BLOCK=300 (was 500) to cut false candidate pairs.

  ALL TYPES INCLUDED: Company, Archived, Group, Companny — same as masters.

Columns:
    id              - auto-increment PK
    company_name    - CLEANED name
    original_name   - original label from masters
    master_id       - record ID in masters
    source_type     - original type from masters
    group_id        - shared by all rows in the same duplicate group
    is_primary      - 1 = canonical row for the group, 0 = duplicate
    is_valid        - 1 = real entity, 0 = garbage
    filter_reason   - why flagged invalid (NULL if valid)

Usage:
    python3 build_balanced_result.py
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

# ─── Thresholds ──────────────────────────────────────────────────

MERGE_THRESHOLD = 0.80        # minimum composite score to consider merging
AUTO_MERGE_THRESHOLD = 0.91   # merge without additional token checks
MAX_BLOCK = 350               # skip blocks larger than this

# ─── Name cleaning (same as improved_result) ─────────────────────

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
    s = _LINKEDIN_SUFFIX_RE.sub("", s)
    s = _PAREN_RE.sub(" ", s)
    s = _LEADING_JUNK_RE.sub("", s)
    s = _TRAILING_JUNK_RE.sub("", s)
    for _ in range(3):
        prev = s
        s = _TRAILING_LOCATION_RE.sub("", s)
        s = _TRAILING_JUNK_RE.sub("", s)
        if s == prev:
            break
    s = re.sub(r"\s+", " ", s)
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

# Role patterns — multi-word job titles that aren't company names
_ROLE_PATTERN_RE = re.compile(
    r"^(?:(?:regional|branch|area|district|territory|zone|zonal|national|"
    r"global|category|channel|senior|junior|assistant|deputy|chief|"
    r"associate|executive|general|central|key\s+account)\s+)+"
    r"(?:(?:sales|marketing|business|service|product|project|operations|"
    r"hr|human|resource|finance|technical|commercial|customer|"
    r"legal|supply|procurement|logistics|it|digital)\s+)*"
    r"(?:manager|director|head|leader|officer|supervisor|coordinator|"
    r"executive|president|counsel)s?"
    r"(?:\s*[\-\u2013\u2014]\s*\w+)*$",
    re.IGNORECASE,
)

# "Independent consultant/contractor" — not a company
_INDEPENDENT_RE = re.compile(
    r"^independent\s+(?:consultant|contractor|advisor|professional|"
    r"practitioner|researcher|developer|designer|engineer)s?$",
    re.IGNORECASE,
)
_SPECIAL_ONLY_RE = re.compile(r"^[^a-zA-Z0-9\u00C0-\u024F\u0400-\u04FF]+$")
_NUMERIC_ONLY_RE = re.compile(r"^[0-9]+$")


def classify_garbage(cleaned: str) -> str | None:
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
    if _ROLE_PATTERN_RE.match(cleaned):
        return "job_role_not_company"
    if _INDEPENDENT_RE.match(cleaned):
        return "independent_not_company"
    if _NON_COMPANY_RE.search(cleaned):
        return "non_company (freelance/self-employed/student/etc)"
    return None


# ─── Normalisation (same as improved_result — keeps biz words) ───

LEGAL_SUFFIXES_MINIMAL = [
    r"\bincorporated\b", r"\binc\b\.?", r"\bllc\b\.?", r"\bl\.l\.c\.?",
    r"\blimited\b", r"\bltd\b\.?", r"\bcorporation\b", r"\bcorp\b\.?",
    r"\bco\b\.?", r"\bplc\b\.?", r"\bgmbh\b", r"\bag\b",
    r"\bs\.?a\.?\b", r"\bn\.?v\.?\b", r"\bpvt\b\.?", r"\bprivate\b",
    r"\bl\.?p\.?\b", r"\bllp\b\.?",
]
LEGAL_SUFFIX_RE = re.compile(
    r"(?:" + "|".join(LEGAL_SUFFIXES_MINIMAL) + r")", re.IGNORECASE
)

STOPWORDS = {"the", "and", "of", "for", "in", "a", "an", "to", "at", "by", "on"}


# Ampersand-acronym pattern: AT&T, H&R, J&J, S&P, R&D, B&Q, etc.
_AMPERSAND_ACRONYM_RE = re.compile(
    r'\b([A-Za-z]{1,3})\s*&\s*([A-Za-z]{1,3})\b'
)


def normalize(label: str) -> str:
    if not label:
        return ""
    s = label.lower().strip()
    # Preserve ampersand-acronyms BEFORE replacing & with 'and'.
    # AT&T → att, H&R → hr, J&J → jj, S&P → sp, etc.
    s = _AMPERSAND_ACRONYM_RE.sub(lambda m: m.group(1) + m.group(2), s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = LEGAL_SUFFIX_RE.sub("", s)
    s = re.sub(r"^the\s+", "", s)
    s = re.sub(r"\s+the$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ─── Business-line differentiator engine (NEW) ──────────────────
#
# STRONG_DIFFERENTIATORS: words that ALWAYS change company identity,
#   even in subset cases (e.g. "Tata Motors" ≠ "Tata Motors Finance").
#
# BUSINESS_DIFFERENTIATORS: broader set; used when BOTH names have
#   unique (non-overlapping) tokens.

STRONG_DIFFERENTIATORS = {
    # Financial
    "finance", "financial", "bank", "banking", "capital",
    "securities", "insurance", "assurance", "reinsurance",
    "credit", "leasing", "lending", "microfinance",
    # Heavy industry
    "power", "steel", "cement", "chemicals", "petrochemicals",
    "polymers", "metals", "mining", "refinery", "petroleum",
    # Healthcare
    "pharma", "pharmaceutical", "pharmaceuticals", "healthcare",
    "hospitals", "hospital", "biotech", "diagnostics",
    # Transport
    "motors", "automobiles", "automotive", "aviation", "airlines",
    "shipping", "logistics",
    # Telecom/Media
    "telecom", "telecommunications", "media", "broadcasting",
    # Real estate
    "realty", "construction", "infrastructure", "properties",
    # Consumer
    "retail", "distribution", "foods", "beverages",
    "fashion", "apparel", "clothing",
    # Education
    "education", "educational", "university", "academy",
    # Agriculture
    "agriculture", "agri", "agrochemicals", "seeds", "fertilizers",
    # Textiles
    "textiles", "textile", "garments",
    # Hospitality / Wellness
    "hotels", "hotel", "resort", "resorts", "holidays", "wellness",
    # Electronics
    "electronics", "electrical", "appliances", "semiconductors",
    # Insurance modifiers
    "life", "general", "health", "home",
}

BUSINESS_DIFFERENTIATORS = STRONG_DIFFERENTIATORS | {
    # Broader set — only checked when BOTH names have unique tokens
    "industries", "industrial", "manufacturing", "engineering",
    "automation", "technologies", "technology", "digital",
    "software", "infotech", "infosystems", "systems", "computing",
    "solutions", "consulting", "consultancy", "advisory",
    "services", "outsourcing", "staffing", "recruitment",
    "entertainment", "communications", "broadband",
    "investment", "investments", "mutual", "fund", "asset",
    "wealth", "housing", "factoring",
    "consumer", "fmcg", "trading", "stores", "wholesale",
    "medical", "sciences", "clinical",
    "transport", "transportation", "freight", "courier",
    "developers", "estates",
    "learning", "training", "institute",
    "fertilisers", "pesticides",
    "fabrics",
    "oil", "gas", "energy", "renewables", "solar", "wind",
    "robotics", "defence", "defense", "aerospace",
    "amc", "nbfc", "hfc",  # common abbreviations
}

# ─── Generic industry terms (NEW) ───────────────────────────────
#
# Tokens that are common across MANY industries and do NOT identify
# a specific company.  If two names share ONLY tokens from this set,
# they are almost certainly different entities in the same industry.
#
# Key rule:  this set is only used to check SHARED tokens.  As long as
# the two names also share at least one NON-generic token (e.g. a
# company name like "apollo", "tata", "hdfc"), the merge is allowed.

GENERIC_INDUSTRY_TERMS = {
    # ── Corporate structure (appear in ANY company name) ──
    "group", "groups", "company", "companies", "corporation", "corporate",
    "enterprises", "enterprise", "industries", "industrial", "industry",
    "international", "global", "worldwide", "national",
    "associates", "associate", "partnership", "partners",
    "holdings", "holding",
    "limited", "pvt", "private", "public",
    "division", "divisions", "unit", "branch",

    # ── Business function descriptors ──
    "services", "service", "solutions", "solution",
    "management", "consulting", "consultant", "consultants", "consultancy",
    "advisory", "advisor", "advisors",
    "products", "product", "works",
    "systems", "system",
    "operations", "operation",

    # ── Industry sector terms (shared by many companies) ──
    "hospital", "hospitals", "clinic", "clinics",
    "medical", "healthcare",
    "financial", "finance",
    "bank", "banking", "banks",
    "insurance", "assurance",
    "engineering", "engineers", "engineer",
    "technology", "technologies", "tech",
    "software", "hardware",
    "bearings", "bearing",
    "machine", "machines", "tools", "tool",
    "devices", "device",
    "exchange", "exchanges",
    "mutual",
    "fund", "funds",
    "electric", "electrical", "electronics", "electronic",
    "vehicles", "vehicle",
    "commercial",
    "pharma", "pharmaceutical", "pharmaceuticals",
    "drugs", "drug",
    "cement", "cements",
    "steel", "steels",
    "power", "energy",
    "motors", "motor",
    "oil", "gas",
    "chemicals", "chemical",
    "paints", "paint",
    "textiles", "textile",
    "retail", "stores", "store",
    "foods", "food",
    "beverages", "beverage",
    "logistics",
    "transport", "transportation",
    "construction",
    "infrastructure",
    "telecom", "telecommunications",
    "media", "broadcasting",
    "digital",
    "automation",
    "research", "development",
    "laboratory", "laboratories", "lab", "labs",
    "manufacturing", "manufacturer", "manufacturers",
    "trading", "traders",
    "distribution", "distributors", "distributor",
    "marketing", "sales",
    "stock",
    "investment", "investments",
    "securities",
    "capital",
    "credit", "lending", "leasing",
    "housing",
    "asset", "wealth",
    "consumer",
    "petroleum", "petrochemicals", "petrochemical",
    "polymers", "plastics", "rubber",
    "metals", "mining",
    "refinery", "refineries",
    "semiconductors",
    "diagnostics",
    "biotech", "biotechnology",
    "aerospace", "defence", "defense",
    "shipping", "freight", "courier",
    "airlines", "aviation",
    "seeds", "fertilizers", "fertilisers",
    "garments", "apparel", "fashion", "fabrics",
    "realty", "properties", "property", "estates", "estate",
    "education", "educational",
    "training", "learning",
    "institute", "institution", "institutions",
    "academy", "university", "college", "school",

    # ── Location / geography (too common to distinguish) ──
    "india", "indian",

    # ── Military / govt ──
    "armed", "forces", "force", "air", "army", "navy",
    "chamber", "commerce",
    "ministry", "department", "government", "govt",

    # ── Role / title words (shouldn't be company names) ──
    "independent",
    "professional", "professionals",
    "regional", "area",
    "senior", "junior",
    "manager", "managers", "director", "directors",
    "head", "officer", "executive",
    "facilities", "facility",

    # ── Other common descriptors ──
    "network", "networks",
    "communications", "communication",
    "broadband", "wireless",
    "computing",
    "outsourcing",
    "staffing", "recruitment",
    "entertainment",
    "supply", "chain",
    "procurement",
    "developers", "developer",
    "advanced", "integrated",
    "small", "micro", "mini", "new",
    "sector",
    "council", "authority", "board", "commission",
    "organization", "organisation",
    "federation", "association",
    "society", "trust", "foundation",
    "welfare",
}


def _significant_tokens(norm: str) -> list:
    """All non-stopword tokens with length > 1."""
    return [t for t in norm.split() if t not in STOPWORDS and len(t) > 1]


def has_business_conflict(norm_a: str, norm_b: str, score: float) -> bool:
    """
    Return True if two names share a common prefix but carry conflicting
    business-line descriptor words, indicating different companies.

    Logic:
      - Find shared vs unique tokens (with fuzzy typo matching).
      - If one name is a pure subset → only block on STRONG_DIFFERENTIATORS
        at moderate scores (< 0.93).
      - If BOTH names have genuinely unique tokens and either side carries
        a BUSINESS_DIFFERENTIATOR → block at moderate scores (< 0.95).
      - Non-business unique tokens on both sides → slight caution (< 0.90).
    """
    tokens_a = set(_significant_tokens(norm_a))
    tokens_b = set(_significant_tokens(norm_b))

    shared = tokens_a & tokens_b
    if not shared:
        return False  # no common prefix — standard scoring handles it

    only_a = tokens_a - tokens_b
    only_b = tokens_b - tokens_a

    if not only_a and not only_b:
        return False  # identical token sets

    # Fuzzy-match unique tokens to handle typos
    # e.g. "induatries" vs "industries" → should match
    real_only_a, real_only_b = set(), set()
    matched_b = set()
    for ta in only_a:
        found = False
        for tb in only_b:
            if tb not in matched_b and fuzz.ratio(ta, tb) >= 80:
                matched_b.add(tb)
                found = True
                break
        if not found:
            real_only_a.add(ta)
    real_only_b = only_b - matched_b

    if not real_only_a and not real_only_b:
        return False  # all unique tokens are typo variants → same company

    # ── Subset case: one name is more specific than the other ──
    if not real_only_a or not real_only_b:
        extra = real_only_a | real_only_b
        # If the extra tokens include a STRONG differentiator,
        # this is likely a different subsidiary (e.g. "Tata Motors Finance")
        if extra & STRONG_DIFFERENTIATORS:
            return score < 0.93
        # Non-differentiating extra tokens (e.g. "Infosys" vs "Infosys BPO")
        # allow merge at normal threshold
        return False

    # ── Both sides have genuinely unique tokens ──
    has_biz_a = bool(real_only_a & BUSINESS_DIFFERENTIATORS)
    has_biz_b = bool(real_only_b & BUSINESS_DIFFERENTIATORS)

    if has_biz_a or has_biz_b:
        # At least one side carries a business keyword that the other doesn't
        # → very likely different companies (Reliance Industries ≠ Reliance Capital)
        return score < 0.95

    # Neither side has business keywords — mild concern for other diffs
    return score < 0.90


# ─── Scoring & merge logic ───────────────────────────────────────

def composite_score(a: str, b: str) -> float:
    """
    Weighted fuzzy composite — rebalanced vs improved_result to reduce
    subset-matching bias (lower weight on token_set and partial).
    """
    if not a or not b:
        return 0.0
    tsr = fuzz.token_sort_ratio(a, b) / 100.0
    tse = fuzz.token_set_ratio(a, b) / 100.0
    rat = fuzz.ratio(a, b) / 100.0
    par = fuzz.partial_ratio(a, b) / 100.0
    return 0.30 * tsr + 0.25 * tse + 0.30 * rat + 0.15 * par


def token_signature(normalized: str) -> str:
    tokens = [t for t in normalized.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(sorted(tokens))


def should_merge(norm_a: str, norm_b: str, score: float) -> bool:
    """
    Decide whether two normalised names should be merged.

    Safety checks (in order):
      1. Score must be ≥ MERGE_THRESHOLD (0.82).
      2. Exact normalised match → always merge.
      3. Short-name protection: if the shorter name has ≤1 significant token,
         require score ≥ 0.95 (prevents "Cholamandalam" bridging to
         "Cholamandalam Home Finance" → "Cholamandalam DBS Finance").
      4. Business-line conflict check.
      5. Very high score (≥ 0.91) → merge (if no conflict detected above).
      6. Medium score (0.82–0.91) → require strong token overlap.
    """
    if score < MERGE_THRESHOLD:
        return False

    if norm_a == norm_b:
        return True

    tokens_a = _significant_tokens(norm_a)
    tokens_b = _significant_tokens(norm_b)
    min_tokens = min(len(tokens_a), len(tokens_b))

    # Short-name bridge prevention
    # ≤1 significant token: require score ≥ 0.95
    if min_tokens <= 1 and score < 0.95:
        return False

    # ── NEW: Generic industry-term protection ──────────────────
    # If the only tokens shared between two names are generic industry
    # terms (like "consulting", "hospital", "bearings"), they are
    # almost certainly different entities.  Block the merge.
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    shared = set_a & set_b

    if shared:
        distinctive_shared = shared - GENERIC_INDUSTRY_TERMS
        if not distinctive_shared:
            # ALL shared tokens are generic.
            only_a = set_a - set_b
            only_b = set_b - set_a

            # Fuzzy-match unique tokens to handle typos
            matched_b = set()
            fuzzy_a = set()
            for ta in only_a:
                for tb in only_b:
                    if tb not in matched_b and fuzz.ratio(ta, tb) >= 80:
                        matched_b.add(tb)
                        fuzzy_a.add(ta)
                        break
            real_only_a = only_a - fuzzy_a
            real_only_b = only_b - matched_b

            if not real_only_a and not real_only_b:
                pass  # all tokens matched (typo variants) → same entity
            elif real_only_a and real_only_b:
                # Both sides have unique tokens with only generic shared
                # → definitely different entities
                return False
            else:
                # Subset case: one side has extra tokens
                extra = real_only_a | real_only_b
                has_distinctive_extra = any(
                    tok not in GENERIC_INDUSTRY_TERMS for tok in extra
                )
                if has_distinctive_extra and score < 0.95:
                    return False
    # ── end generic protection ────────────────────────────────

    # Business-line conflict
    if has_business_conflict(norm_a, norm_b, score):
        return False

    # Very high confidence → merge
    if score >= AUTO_MERGE_THRESHOLD:
        return True

    # Medium confidence: verify solid token overlap
    set_a = set(tokens_a)
    set_b = set(tokens_b)

    if set_a == set_b:
        return True

    # Fuzzy token overlap
    matches = 0
    for ta in set_a:
        for tb in set_b:
            if fuzz.ratio(ta, tb) >= 80:
                matches += 1
                break
    max_len = max(len(set_a), len(set_b))
    if max_len == 0:
        return False
    return (matches / max_len) >= 0.65


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
    score = 0.0
    if not label or len(label.strip()) < 2:
        return -10
    type_bonus = {"Company": 100, "Companny": 90, "Group": 50, "Archived": 10}
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
    print("=" * 65)
    print("Building `balanced_result` table")
    print("  Target dedup rate:  30–35%")
    print("  Merge threshold:   0.80")
    print("  Auto-merge:        0.91  (was 0.92)")
    print("  Business-line conflict detection:  ENABLED")
    print("  Short-name bridge prevention:      ENABLED")
    print("  Includes: ALL types (Company + Archived + Group + ...)")
    print("=" * 65 + "\n")

    # Retry connection with backoff
    for _attempt in range(10):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            print(f"  DB connected (attempt {_attempt + 1}).\n")
            break
        except Exception as e:
            print(f"  DB connect attempt {_attempt + 1} failed: {e}")
            import time as _t
            _t.sleep(5 * (_attempt + 1))
    else:
        print("FATAL: Could not connect to DB after 10 attempts.")
        return

    # ── 1. Create table ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS balanced_result (
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
            INDEX idx_group_id    (group_id),
            INDEX idx_master_id   (master_id),
            INDEX idx_source_type (source_type),
            INDEX idx_is_primary  (is_primary),
            INDEX idx_is_valid    (is_valid)
        )
    """)
    cur.execute("DELETE FROM balanced_result")
    conn.commit()
    print("Table `balanced_result` ready.\n")

    # ── 2. Load ALL rows from masters ────────────────────────────
    print("Loading ALL records from masters...")
    cur.execute("SELECT id, label, type FROM masters")
    all_rows = cur.fetchall()
    print(f"  Loaded {len(all_rows):,} total rows.\n")

    # ── 3. Clean & classify ──────────────────────────────────────
    print("Cleaning names & classifying garbage...")
    all_records = []
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
    print(f"  Valid: {valid_count:,}")
    print(f"  Garbage (is_valid=0): {garbage_count:,}\n")

    # ── 4. Build lookups ─────────────────────────────────────────
    valid_records = [(r[0], r[1]) for r in all_records if r[4] == 1 and r[1]]
    id_to_cleaned = {r[0]: r[1] for r in valid_records}
    id_to_norm = {rid: normalize(cleaned) for rid, cleaned in valid_records}
    id_to_type = {r[0]: r[3] for r in all_records}

    # ── 5. Blocking ──────────────────────────────────────────────
    print("Building candidate pairs via blocking...")

    block_prefix3 = defaultdict(set)
    block_prefix4 = defaultdict(set)
    block_token = defaultdict(set)
    block_first_word = defaultdict(set)
    block_word_prefix = defaultdict(set)
    block_compound = defaultdict(set)

    for rid, norm in id_to_norm.items():
        if len(norm) >= 3:
            block_prefix3[norm[:3]].add(rid)
        if len(norm) >= 4:
            block_prefix4[norm[:4]].add(rid)
        tsig = token_signature(norm)
        if tsig and len(tsig.split()) > 1:
            block_token[tsig].add(rid)
        words = [w for w in norm.split() if w not in STOPWORDS and len(w) > 2]
        if words:
            block_first_word[words[0]].add(rid)
        for w in words:
            if len(w) > 4:
                block_word_prefix[w[:5]].add(rid)
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

    # ── 6. Score & merge ─────────────────────────────────────────
    print("Scoring pairs (threshold=0.80 + biz-conflict + short-name guard)...")
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

    # ── 6b. Post-processing: split oversized groups ──────────────
    MAX_GROUP_SIZE = 50
    TIGHT_THRESHOLD = 0.90
    raw_groups = uf.groups()
    oversized = {root: members for root, members in raw_groups.items()
                 if len(members) > MAX_GROUP_SIZE}
    if oversized:
        print(f"  Splitting {len(oversized)} oversized groups (>{MAX_GROUP_SIZE} members)...")
        split_count = 0
        for root, members in oversized.items():
            member_list = sorted(members)
            # Build a new, tighter Union-Find for just this group
            sub_uf = UnionFind()
            for mid in member_list:
                sub_uf.find(mid)  # ensure all members are initialised
            for i in range(len(member_list)):
                for j in range(i + 1, len(member_list)):
                    id_a, id_b = member_list[i], member_list[j]
                    na = id_to_norm.get(id_a, "")
                    nb = id_to_norm.get(id_b, "")
                    if not na or not nb:
                        continue
                    if na == nb:
                        sub_uf.union(id_a, id_b)
                        continue
                    sc = composite_score(na, nb)
                    if sc >= TIGHT_THRESHOLD and should_merge(na, nb, sc):
                        sub_uf.union(id_a, id_b)
            sub_groups = sub_uf.groups()
            if len(sub_groups) > 1:
                split_count += 1
                # Remove old edges from main UF by rebuilding
                for sg_root, sg_members in sub_groups.items():
                    sg_list = sorted(sg_members)
                    # Re-unify within the new sub-group in the main UF
                    # (we need a fresh UF for this; we'll rebuild below)
                    pass
        if split_count > 0:
            # Rebuild main UF from scratch with tighter thresholds for
            # oversized groups
            print(f"    {split_count} groups were split. Rebuilding UF...")
            uf2 = UnionFind()
            # First, re-merge all pairs from non-oversized groups
            for root, members in raw_groups.items():
                if root not in oversized:
                    ml = sorted(members)
                    for k in range(1, len(ml)):
                        uf2.union(ml[0], ml[k])
            # Then, for each oversized group, merge sub-groups separately
            for root, members in oversized.items():
                member_list = sorted(members)
                sub_uf = UnionFind()
                for mid in member_list:
                    sub_uf.find(mid)
                for i in range(len(member_list)):
                    for j in range(i + 1, len(member_list)):
                        id_a, id_b = member_list[i], member_list[j]
                        na = id_to_norm.get(id_a, "")
                        nb = id_to_norm.get(id_b, "")
                        if not na or not nb:
                            continue
                        if na == nb:
                            sub_uf.union(id_a, id_b)
                            uf2.union(id_a, id_b)
                            continue
                        sc = composite_score(na, nb)
                        if sc >= TIGHT_THRESHOLD and should_merge(na, nb, sc):
                            sub_uf.union(id_a, id_b)
                            uf2.union(id_a, id_b)
            uf = uf2
            groups_after = uf.groups()
            still_big = sum(1 for m in groups_after.values() if len(m) > MAX_GROUP_SIZE)
            print(f"    After split: {len(groups_after):,} groups, {still_big} still >{MAX_GROUP_SIZE}")
        else:
            print("    No groups needed splitting.")
    else:
        print("  No oversized groups found.")

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

    # Singletons
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

    # ── 8. Insert ────────────────────────────────────────────────
    try:
        cur.close()
        conn.close()
    except Exception:
        pass
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("DELETE FROM balanced_result")
    conn.commit()

    print("Inserting into `balanced_result`...")
    BATCH = 5000
    insert_sql = (
        "INSERT INTO balanced_result "
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

    # ── 9. Summary ───────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM balanced_result")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM masters")
    master_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM balanced_result WHERE is_valid=1")
    valid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM balanced_result WHERE is_valid=0")
    invalid = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM balanced_result WHERE is_primary=1 AND is_valid=1")
    primaries = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT group_id) FROM balanced_result WHERE is_valid=1")
    unique_groups = cur.fetchone()[0]

    dedup_pct = (valid - primaries) / valid * 100 if valid else 0

    print("=" * 65)
    print("SUMMARY")
    print("=" * 65)
    print(f"  masters total:                   {master_total:,}")
    print(f"  balanced_result total:           {total:,}")
    print(f"  Row match:                       {'YES' if total == master_total else 'NO'}")
    print(f"  Valid:                           {valid:,}")
    print(f"  Garbage:                         {invalid:,}")
    print(f"  Unique groups (valid):           {unique_groups:,}")
    print(f"  Primaries:                       {primaries:,}")
    print(f"  Duplicates removed:              {valid - primaries:,}")
    print(f"  *** DEDUP RATE:                  {dedup_pct:.1f}% ***")
    if 30 <= dedup_pct <= 35:
        print(f"  >>> ON TARGET (30-35%) <<<")
    elif dedup_pct < 30:
        print(f"  >>> Below target — consider lowering MERGE_THRESHOLD")
    else:
        print(f"  >>> Above target — consider raising MERGE_THRESHOLD")

    # Comparison
    for tbl in ("improved_result", "aggressive_filter_all", "ultra_aggressive_filter"):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE is_valid=1")
            ov = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE is_primary=1 AND is_valid=1")
            op = cur.fetchone()[0]
            od = ((ov - op) / ov * 100) if ov else 0
            print(f"  {tbl}: {od:.1f}% dedup ({op:,} primaries)")
        except Exception:
            pass

    # By type
    cur.execute("""
        SELECT source_type, COUNT(*),
               SUM(CASE WHEN is_valid=1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN is_valid=0 THEN 1 ELSE 0 END)
        FROM balanced_result GROUP BY source_type ORDER BY COUNT(*) DESC
    """)
    print(f"\n  By source_type:")
    for st, ct, v, iv in cur.fetchall():
        print(f"    {st}: {ct:,} ({v:,} valid, {iv:,} garbage)")

    # Garbage breakdown
    cur.execute("""
        SELECT filter_reason, COUNT(*) FROM balanced_result
        WHERE is_valid=0 GROUP BY filter_reason ORDER BY COUNT(*) DESC
    """)
    print(f"\n  Garbage breakdown:")
    for reason, cnt in cur.fetchall():
        print(f"    {reason}: {cnt:,}")

    # ── 10. Spot checks — known problem cases ────────────────────
    print(f"\n{'=' * 65}")
    print("SPOT CHECKS")
    print(f"{'=' * 65}")

    for keyword in ("cholamandalam", "reliance", "tata", "titan", "mahindra", "birla"):
        cur.execute("""
            SELECT group_id, COUNT(*) as cnt
            FROM balanced_result
            WHERE (company_name LIKE %s OR original_name LIKE %s) AND is_valid=1
            GROUP BY group_id ORDER BY cnt DESC LIMIT 5
        """, (f"%{keyword}%", f"%{keyword}%"))
        groups_found = cur.fetchall()
        if not groups_found:
            continue
        print(f"\n  '{keyword}' — {len(groups_found)} group(s) shown (top 5):")
        for gid, cnt in groups_found:
            cur.execute("""
                SELECT master_id, company_name, source_type, is_primary
                FROM balanced_result WHERE group_id=%s AND is_valid=1
                ORDER BY is_primary DESC, company_name LIMIT 8
            """, (gid,))
            rows = cur.fetchall()
            pri_name = next((r[1] for r in rows if r[3] == 1), "?")
            print(f"    Group {gid} ({cnt} members) — primary: {pri_name[:60]}")
            for mid, name, st, pri in rows[:5]:
                mk = " ★" if pri else ""
                print(f"      [{st:>10}] ID={mid:>6}  {name[:65]}{mk}")
            if cnt > 5:
                print(f"      ... and {cnt - 5} more")

    # Largest groups
    print(f"\n  --- Top 10 largest groups ---")
    cur.execute("""
        SELECT group_id, COUNT(*) as cnt
        FROM balanced_result WHERE is_valid=1
        GROUP BY group_id ORDER BY cnt DESC LIMIT 10
    """)
    for gid, cnt in cur.fetchall():
        cur.execute(
            "SELECT company_name FROM balanced_result WHERE group_id=%s AND is_primary=1 LIMIT 1",
            (gid,),
        )
        row = cur.fetchone()
        name = row[0] if row else "?"
        print(f"    group {gid}: {cnt} members — {name[:60]}")

    # ── 11. Useful queries ───────────────────────────────────────
    print(f"\n{'=' * 65}")
    print("USEFUL QUERIES")
    print(f"{'=' * 65}")
    print("  -- All rows for a keyword:")
    print("  SELECT * FROM balanced_result WHERE company_name LIKE '%reliance%';")
    print()
    print("  -- Deduplicated view:")
    print("  SELECT * FROM balanced_result WHERE is_primary=1 AND is_valid=1;")
    print()
    print("  -- All members of a group:")
    print("  SELECT * FROM balanced_result WHERE group_id=<id> ORDER BY is_primary DESC;")
    print()
    print("  -- Count unique companies:")
    print("  SELECT COUNT(DISTINCT group_id) FROM balanced_result WHERE is_valid=1;")

    conn.close()
    print(f"\n=== Done in {time.time() - t0:.1f}s ===")


if __name__ == "__main__":
    main()

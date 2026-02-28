#!/usr/bin/env python3
"""
Company Deduplication Workflow
==============================
Identifies duplicate company records in the `masters` table of the
dedup_infollion database.  Uses text normalisation, fuzzy matching
(rapidfuzz), Union-Find grouping, and AI validation for ambiguous cases.

By default uses a fast LOCAL heuristic AI engine.  Set USE_OPENROUTER=True
to use the OpenRouter / Llama 3.1 70B model instead (slower but available
for others who need cloud-based validation).

Usage:
    python3 dedup.py
"""

import os
import re
import sys
import json
import time
import logging
from collections import defaultdict
from itertools import combinations

import mysql.connector
import requests
from rapidfuzz import fuzz
from dotenv import load_dotenv

# ────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connection_timeout": 120,
    "autocommit": False,
}

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.1-70b-instruct")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Toggle: False = fast local heuristic AI, True = OpenRouter cloud AI (slower)
USE_OPENROUTER = False

# Thresholds (0-1 scale)
THRESHOLD_AUTO_DUPLICATE = 0.92   # >= this → auto-confirmed duplicate
THRESHOLD_PROBABLE       = 0.75   # >= this & < auto → send to AI
# Below THRESHOLD_PROBABLE → discard pair

AI_CONFIDENCE_ACCEPT = 0.7       # AI says same_company=true & conf >= this → duplicate
MAX_AI_CALLS = 200               # Cap on AI API calls to control cost
AI_CALL_DELAY = 1.0              # Seconds between AI calls

# Blocking: max block size to consider (skip overly large blocks)
MAX_BLOCK_SIZE = 500

# Primary quality threshold — below this, create a new cleaned record
PRIMARY_QUALITY_MIN = 1

# Group coherence: minimum composite score between a member and the primary.
# Members below this get kicked out of the group (transitive false positives).
GROUP_COHERENCE_MIN = 0.60

# Batch size for DB updates (bulk CASE/WHEN queries)
DB_BATCH_SIZE = 2000

# ────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dedup")

# ────────────────────────────────────────────────────────────────────
# Legal suffix patterns
# ────────────────────────────────────────────────────────────────────
# Core legal suffixes only — avoid stripping words that are often part of company names
# (e.g. "Global DB Solutions" should stay "global db solutions", not become "db")
LEGAL_SUFFIXES = [
    r"\bincorporated\b", r"\binc\b\.?", r"\bllc\b\.?", r"\bl\.l\.c\.?",
    r"\blimited\b", r"\bltd\b\.?", r"\bcorporation\b", r"\bcorp\b\.?",
    r"\bcompany\b", r"\bco\b\.?", r"\bplc\b\.?", r"\bgmbh\b",
    r"\bag\b", r"\bs\.?a\.?\b", r"\bn\.?v\.?\b", r"\bpvt\b\.?",
    r"\bprivate\b", r"\bl\.?p\.?\b", r"\bllp\b\.?", r"\bgroup\b",
    r"\bholdings?\b", r"\benterprise[s]?\b", r"\binternational\b",
    # Excluded: global, services, solutions — often core to company name
    r"\btechnolog(?:y|ies)\b", r"\bconsulting\b", r"\badvisors?\b",
    r"\bindustries\b",
]
LEGAL_SUFFIX_RE = re.compile(
    r"(?:" + "|".join(LEGAL_SUFFIXES) + r")", re.IGNORECASE
)

STOPWORDS = {"the", "and", "of", "for", "in", "a", "an", "to", "at", "by", "on"}

# ────────────────────────────────────────────────────────────────────
# Text normalisation
# ────────────────────────────────────────────────────────────────────

def normalize(label: str) -> str:
    """Return a normalised version of a company label for comparison."""
    if not label:
        return ""
    s = label.lower().strip()
    # Replace & with and
    s = s.replace("&", " and ")
    # Remove punctuation except spaces
    s = re.sub(r"[^\w\s]", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # Remove legal suffixes
    s = LEGAL_SUFFIX_RE.sub("", s)
    # Remove leading/trailing 'the'
    s = re.sub(r"^the\s+", "", s)
    s = re.sub(r"\s+the$", "", s)
    # Final cleanup
    s = re.sub(r"\s+", " ", s).strip()
    return s


def token_signature(normalized: str) -> str:
    """Sorted non-stopword tokens joined — used as a blocking key."""
    tokens = [t for t in normalized.split() if t not in STOPWORDS and len(t) > 1]
    return " ".join(sorted(tokens))


def _is_generic_block_key(key: str) -> bool:
    """Skip blocks that are too short/generic (e.g. single token 'db', 'hinduja')."""
    if not key or len(key) < 4:
        return True
    tokens = key.split()
    if len(tokens) == 1 and len(tokens[0]) <= 3:
        return True  # e.g. "db"
    if len(tokens) == 1:
        return True  # single-token blocks cause false positives (e.g. "hinduja")
    return False

# ────────────────────────────────────────────────────────────────────
# Similarity scoring
# ────────────────────────────────────────────────────────────────────

def composite_score(a: str, b: str) -> float:
    """Weighted composite of multiple fuzzy metrics (0-1 scale)."""
    if not a or not b:
        return 0.0
    tsr = fuzz.token_sort_ratio(a, b) / 100.0
    tse = fuzz.token_set_ratio(a, b) / 100.0
    rat = fuzz.ratio(a, b) / 100.0
    par = fuzz.partial_ratio(a, b) / 100.0
    return 0.30 * tsr + 0.30 * tse + 0.20 * rat + 0.20 * par

# ────────────────────────────────────────────────────────────────────
# Union-Find
# ────────────────────────────────────────────────────────────────────

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
        """Return dict: root -> set of members."""
        g = defaultdict(set)
        for x in self.parent:
            g[self.find(x)].add(x)
        return {k: v for k, v in g.items() if len(v) > 1}

# ────────────────────────────────────────────────────────────────────
# LOCAL heuristic AI (instant, no API calls)
# ────────────────────────────────────────────────────────────────────

def _initials(name: str) -> str:
    """Extract uppercase initials, e.g. 'Tata Consultancy Services' -> 'TCS'."""
    return "".join(w[0] for w in name.split() if w and w[0].isalpha()).upper()


def local_ai_validate(label_a, label_b, norm_a, norm_b, score):
    """Rule-based heuristic that mimics AI validation for company dedup.
    Uses multiple signals beyond the raw fuzzy score."""

    # 1) Exact normalised match
    if norm_a == norm_b:
        return {"same_company": True, "confidence": 0.99,
                "reasoning": "Normalised names identical."}

    ta = set(norm_a.split()) - STOPWORDS
    tb = set(norm_b.split()) - STOPWORDS

    # 2) Token-set containment
    if ta and tb and (ta <= tb or tb <= ta):
        ov = len(ta & tb) / max(len(ta), len(tb))
        if ov >= 0.6 and score >= 0.78:
            return {"same_company": True, "confidence": round(0.80 + ov * 0.15, 2),
                    "reasoning": f"Token containment ({ov:.0%}), score {score:.3f}."}

    # 3) Substring
    la, lb = label_a.lower().strip(), label_b.lower().strip()
    sh, lo = (la, lb) if len(la) <= len(lb) else (lb, la)
    if len(sh) >= 4 and sh in lo:
        return {"same_company": True, "confidence": round(0.82 + score * 0.10, 2),
                "reasoning": f"Substring match, score {score:.3f}."}

    # 4) Abbreviation / initials
    if ta and tb:
        ia, ib = _initials(label_a), _initials(label_b)
        sl = label_a.strip() if len(label_a.strip()) < len(label_b.strip()) else label_b.strip()
        li = ia if sl == label_b.strip() else ib
        su = sl.upper().replace(".", "").replace(" ", "")
        if 2 <= len(su) <= 5 and su == li and score >= 0.75:
            return {"same_company": True, "confidence": 0.80,
                    "reasoning": "Initials match."}

    # 5) High score + token overlap
    if score >= 0.82 and ta and tb:
        ov = len(ta & tb) / min(len(ta), len(tb))
        if ov >= 0.5:
            return {"same_company": True, "confidence": round(score * 0.95, 2),
                    "reasoning": f"High score ({score:.3f}), {ov:.0%} token overlap."}

    # 6) Moderate score
    if score >= 0.78 and ta and tb:
        ov = len(ta & tb) / min(len(ta), len(tb))
        if ov >= 0.3:
            return {"same_company": True, "confidence": round(score * 0.80, 2),
                    "reasoning": f"Moderate score ({score:.3f}), {ov:.0%} overlap."}

    # Default: not the same
    return {"same_company": False, "confidence": round(1.0 - score, 2),
            "reasoning": f"Score {score:.3f} below threshold."}


# ────────────────────────────────────────────────────────────────────
# OpenRouter AI (kept for cloud-based validation — set USE_OPENROUTER=True)
# ────────────────────────────────────────────────────────────────────

def openrouter_ai_validate(label_a: str, label_b: str) -> dict:
    """Ask LLM whether two company names refer to the same entity.
    Returns dict with keys: same_company, confidence, reasoning.
    """
    prompt = (
        "You are a data quality expert. Determine whether these two company "
        "names refer to the same real-world company.\n\n"
        f'Company A: "{label_a}"\n'
        f'Company B: "{label_b}"\n\n'
        "Respond in this exact JSON format and nothing else:\n"
        '{"same_company": true/false, "confidence": 0.0-1.0, "reasoning": "..."}'
    )

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://dedup-infollion.local",
    }
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 300,
    }

    for attempt in range(3):
        try:
            resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt * 2
                log.warning("Rate-limited, retrying in %ds…", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            # Try to parse JSON from the response
            # Handle markdown code blocks
            if "```" in content:
                content = re.search(r"```(?:json)?\s*(.*?)```", content, re.DOTALL)
                if content:
                    content = content.group(1).strip()
                else:
                    content = resp.json()["choices"][0]["message"]["content"].strip()
            result = json.loads(content)
            return {
                "same_company": bool(result.get("same_company", False)),
                "confidence": float(result.get("confidence", 0.0)),
                "reasoning": str(result.get("reasoning", "")),
            }
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            log.warning("AI response parse error (attempt %d): %s", attempt + 1, exc)
            # Return a conservative result
            return {
                "same_company": False,
                "confidence": 0.0,
                "reasoning": f"Parse error: {exc}",
            }
        except requests.RequestException as exc:
            log.warning("AI request error (attempt %d): %s", attempt + 1, exc)
            time.sleep(2 ** attempt)

    return {"same_company": False, "confidence": 0.0, "reasoning": "All retries failed"}

# ────────────────────────────────────────────────────────────────────
# Primary record selection
# ────────────────────────────────────────────────────────────────────

_LEGAL_SUFFIX_SIMPLE = re.compile(
    r"\b(?:inc|ltd|llc|corp|plc|gmbh|pvt|limited|corporation|incorporated)\b",
    re.IGNORECASE,
)
_GARBAGE_RE = re.compile(
    r"^(?:test|unknown|company\s*\d*|n/?a|none|--|-)$", re.IGNORECASE
)

def primary_score(label: str) -> int:
    """Score a label's quality for primary record selection (higher = better)."""
    score = 0
    if not label or len(label.strip()) < 2:
        return -10

    # Proper casing
    if label == label.upper():
        score -= 1  # ALL CAPS
    elif label == label.lower():
        score -= 1  # all lowercase
    elif label[0].isupper():
        score += 2  # Title-ish case

    # Has legal suffix (completeness)
    if _LEGAL_SUFFIX_SIMPLE.search(label):
        score += 1

    # Length — prefer fuller names (max bonus at ~30 chars)
    length = len(label.strip())
    if length >= 5:
        score += min(length / 30.0, 1.0)

    # Cleanliness
    if "  " in label or label != label.strip():
        score -= 1
    if re.search(r"[^\w\s.,&()\-/']", label):
        score -= 1  # Extraneous characters

    # Garbage patterns
    if _GARBAGE_RE.match(label.strip()):
        score -= 5

    return score

# ────────────────────────────────────────────────────────────────────
# Schema migration (idempotent)
# ────────────────────────────────────────────────────────────────────

def ensure_schema(cur):
    """Add dedup columns if they don't already exist."""
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'masters' "
        "AND COLUMN_NAME = 'duplicate_status'",
        (DB_CONFIG["database"],),
    )
    if cur.fetchone():
        log.info("Schema columns already exist — skipping migration.")
        return
    log.info("Running schema migration…")
    cur.execute("""
        ALTER TABLE masters
        ADD COLUMN duplicate_status ENUM('primary','duplicate','probable') DEFAULT NULL,
        ADD COLUMN duplicate_of INT NULL,
        ADD COLUMN duplicate_score DECIMAL(5,3) NULL,
        ADD COLUMN ai_decision TEXT NULL,
        ADD COLUMN record_type ENUM('new','old') DEFAULT 'old'
    """)
    log.info("Schema migration complete.")

# ────────────────────────────────────────────────────────────────────
# Main workflow
# ────────────────────────────────────────────────────────────────────

def main():
    t_start = time.time()
    log.info("=== Company Deduplication Workflow ===")
    log.info("AI engine: %s", "OpenRouter / " + OPENROUTER_MODEL if USE_OPENROUTER else "Local heuristic")

    # ── Connect ──────────────────────────────────────────────────
    log.info("Connecting to database…")
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ── Schema migration ─────────────────────────────────────────
    ensure_schema(cur)
    conn.commit()

    # ── Reset previous dedup state ───────────────────────────────
    log.info("Resetting previous dedup status on Company rows…")
    # Remove any 'new' primary records created by previous runs
    cur.execute(
        "DELETE FROM masters WHERE record_type = 'new' AND type = 'Company'"
    )
    deleted_new = cur.rowcount
    if deleted_new:
        log.info("Deleted %d 'new' records from previous run.", deleted_new)
    cur.execute(
        "UPDATE masters SET duplicate_status = NULL, duplicate_of = NULL, "
        "duplicate_score = NULL, ai_decision = NULL, record_type = 'old' "
        "WHERE type = 'Company' AND duplicate_status IS NOT NULL"
    )
    conn.commit()
    log.info("Reset %d rows.", cur.rowcount)

    # ── Load data ────────────────────────────────────────────────
    log.info("Loading company records…")
    cur.execute("SELECT id, label FROM masters WHERE type = 'Company'")
    rows = cur.fetchall()
    log.info("Loaded %d company records.", len(rows))

    # Close initial connection — the scoring/AI phase is long and will
    # cause the connection to time out.  We'll reconnect for DB updates.
    cur.close()
    conn.close()
    log.info("Closed initial DB connection (will reconnect for updates).")

    # Build lookup: id -> label, id -> normalized
    id_to_label = {}
    id_to_norm = {}
    for rid, label in rows:
        if not label or len(label.strip()) < 2:
            continue
        id_to_label[rid] = label
        id_to_norm[rid] = normalize(label)

    log.info("After filtering short/empty labels: %d records.", len(id_to_label))

    # ── Blocking ─────────────────────────────────────────────────
    log.info("Building blocking index…")
    block_prefix = defaultdict(set)   # first 3 chars -> set of ids
    block_token = defaultdict(set)    # sorted token sig -> set of ids

    for rid, norm in id_to_norm.items():
        if len(norm) >= 3:
            block_prefix[norm[:3]].add(rid)
        tsig = token_signature(norm)
        if tsig and not _is_generic_block_key(tsig):
            block_token[tsig].add(rid)

    # Collect candidate pairs from blocks
    log.info("Generating candidate pairs from %d prefix blocks + %d token blocks…",
             len(block_prefix), len(block_token))

    candidate_pairs = set()
    for block_dict in (block_prefix, block_token):
        for key, members in block_dict.items():
            if len(members) < 2 or len(members) > MAX_BLOCK_SIZE:
                continue
            members_list = sorted(members)
            for i in range(len(members_list)):
                for j in range(i + 1, len(members_list)):
                    candidate_pairs.add((members_list[i], members_list[j]))

    log.info("Total candidate pairs: %d", len(candidate_pairs))

    # ── Scoring ──────────────────────────────────────────────────
    log.info("Scoring candidate pairs…")
    auto_dup_pairs = []    # (id_a, id_b, score) — score >= THRESHOLD_AUTO_DUPLICATE
    probable_pairs = []    # (id_a, id_b, score) — THRESHOLD_PROBABLE <= score < auto

    scored_count = 0
    for id_a, id_b in candidate_pairs:
        norm_a = id_to_norm.get(id_a, "")
        norm_b = id_to_norm.get(id_b, "")
        if not norm_a or not norm_b:
            continue
        # Quick pre-filter: if normalised strings are identical → auto match
        if norm_a == norm_b:
            auto_dup_pairs.append((id_a, id_b, 1.0))
            scored_count += 1
            continue

        score = composite_score(norm_a, norm_b)
        if score >= THRESHOLD_AUTO_DUPLICATE:
            auto_dup_pairs.append((id_a, id_b, score))
        elif score >= THRESHOLD_PROBABLE:
            probable_pairs.append((id_a, id_b, score))
        scored_count += 1
        if scored_count % 500000 == 0:
            log.info("  Scored %d pairs so far…", scored_count)

    log.info("Scoring complete. Auto-dup: %d, Probable: %d, Total scored: %d",
             len(auto_dup_pairs), len(probable_pairs), scored_count)

    # ── Union-Find: merge auto-confirmed duplicates ──────────────
    uf = UnionFind()
    pair_scores = {}   # (id_a, id_b) -> score (best)
    pair_ai = {}       # (id_a, id_b) -> ai_decision json string

    for id_a, id_b, score in auto_dup_pairs:
        uf.union(id_a, id_b)
        key = (min(id_a, id_b), max(id_a, id_b))
        pair_scores[key] = max(pair_scores.get(key, 0), score)

    log.info("After auto-dup pass: %d groups.", len(uf.groups()))

    # ── AI validation for probable pairs ─────────────────────────
    # Sort by score descending so we validate best candidates first
    probable_pairs.sort(key=lambda x: x[2], reverse=True)
    ai_calls_made = 0
    ai_confirmed = 0
    ai_rejected = 0
    ai_probable = 0

    ai_cap = MAX_AI_CALLS if USE_OPENROUTER else len(probable_pairs)
    log.info("Starting AI validation for up to %d probable pairs (cap=%d, engine=%s)…",
             len(probable_pairs), ai_cap,
             "OpenRouter" if USE_OPENROUTER else "local")

    for id_a, id_b, score in probable_pairs:
        if ai_calls_made >= ai_cap:
            break
        # Skip if already in the same group
        if uf.find(id_a) == uf.find(id_b):
            continue

        label_a = id_to_label[id_a]
        label_b = id_to_label[id_b]
        norm_a = id_to_norm[id_a]
        norm_b = id_to_norm[id_b]

        if USE_OPENROUTER:
            result = openrouter_ai_validate(label_a, label_b)
        else:
            result = local_ai_validate(label_a, label_b, norm_a, norm_b, score)
        ai_calls_made += 1

        key = (min(id_a, id_b), max(id_a, id_b))
        pair_scores[key] = max(pair_scores.get(key, 0), score)
        pair_ai[key] = json.dumps(result)

        if result["same_company"] and result["confidence"] >= AI_CONFIDENCE_ACCEPT:
            uf.union(id_a, id_b)
            ai_confirmed += 1
        elif result["same_company"]:
            ai_probable += 1
        else:
            ai_rejected += 1

        log_interval = 10 if USE_OPENROUTER else 50000
        if ai_calls_made % log_interval == 0:
            log.info("  AI calls: %d (confirmed=%d, rejected=%d, probable=%d)",
                     ai_calls_made, ai_confirmed, ai_rejected, ai_probable)

        if USE_OPENROUTER:
            time.sleep(AI_CALL_DELAY)

    log.info("AI validation done. Calls=%d, Confirmed=%d, Rejected=%d, Probable=%d",
             ai_calls_made, ai_confirmed, ai_rejected, ai_probable)

    # ── Build final groups ───────────────────────────────────────
    groups = uf.groups()
    log.info("Total duplicate groups: %d", len(groups))

    # ── Select primary for each group ────────────────────────────
    group_info = []  # list of dicts with primary_id, members, scores, etc.

    for root, members in groups.items():
        # Score each member
        scored_members = [
            (mid, id_to_label.get(mid, ""), primary_score(id_to_label.get(mid, "")))
            for mid in members
        ]
        scored_members.sort(key=lambda x: x[2], reverse=True)
        best_id, best_label, best_score = scored_members[0]

        # Determine if we need a new cleaned primary record
        new_record = None
        if best_score < PRIMARY_QUALITY_MIN:
            cleaned = best_label.strip()
            if cleaned and cleaned != "-":
                cleaned = cleaned.title()
                new_record = cleaned

        ginfo = {
            "primary_id": best_id,
            "primary_label": best_label,
            "new_record_label": new_record,
            "members": [],
        }
        for mid, mlabel, mscore in scored_members:
            if mid == best_id and new_record is None:
                continue  # primary — handled separately
            key = (min(best_id, mid), max(best_id, mid))
            dup_score = pair_scores.get(key, 0)
            ai_dec = pair_ai.get(key)
            # If no direct pair score (transitive merge), compute it now
            if dup_score == 0 and mid != best_id:
                n_a = id_to_norm.get(best_id, "")
                n_b = id_to_norm.get(mid, "")
                if n_a and n_b:
                    dup_score = round(composite_score(n_a, n_b), 3)
            if mid == best_id and new_record is not None:
                dup_score = 1.0  # original best becomes dup of new primary
            # Coherence filter: skip members too dissimilar to the primary
            if dup_score < GROUP_COHERENCE_MIN and mid != best_id:
                continue
            ginfo["members"].append({
                "id": mid,
                "label": mlabel,
                "score": dup_score,
                "ai_decision": ai_dec,
            })

        # Only keep groups with at least 1 duplicate member
        if ginfo["members"]:
            group_info.append(ginfo)

    # ── Database updates ─────────────────────────────────────────
    log.info("Updating database with %d groups…", len(group_info))

    # Reconnect to MySQL for the update phase
    log.info("Reconnecting to MySQL for updates…")
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    conn.ping(reconnect=True)
    log.info("Reconnected successfully.")

    # Collect all updates
    primary_updates = []    # (id,)
    duplicate_updates = []  # (primary_id, score, ai_decision, id)
    new_inserts = []        # groups needing a new primary record
    probable_updates = []   # (score, ai_decision, id)

    for ginfo in group_info:
        if ginfo["new_record_label"]:
            new_inserts.append(ginfo)
        else:
            primary_updates.append((ginfo["primary_id"],))
            for m in ginfo["members"]:
                duplicate_updates.append((
                    ginfo["primary_id"],
                    m["score"],
                    m["ai_decision"],
                    m["id"],
                ))

    # Handle probable pairs that were NOT confirmed/grouped
    probable_ids_to_update = set()
    for id_a, id_b, score in probable_pairs:
        key = (min(id_a, id_b), max(id_a, id_b))
        ai_dec = pair_ai.get(key)
        if ai_dec:
            parsed = json.loads(ai_dec)
            if parsed.get("same_company") and parsed.get("confidence", 0) < AI_CONFIDENCE_ACCEPT:
                # Mark both as probable
                for pid in (id_a, id_b):
                    if pid not in probable_ids_to_update:
                        probable_updates.append((score, ai_dec, pid))
                        probable_ids_to_update.add(pid)

    # Execute updates in batches using bulk CASE/WHEN SQL
    # ─── Primary updates (simple: all get same values) ───
    log.info("  Setting %d primary records…", len(primary_updates))
    total_p_batches = (len(primary_updates) + DB_BATCH_SIZE - 1) // DB_BATCH_SIZE
    for i in range(0, len(primary_updates), DB_BATCH_SIZE):
        batch = primary_updates[i:i + DB_BATCH_SIZE]
        ids = [b[0] for b in batch]
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(
            f"UPDATE masters SET duplicate_status = 'primary', duplicate_of = NULL, "
            f"record_type = 'old' WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
        bn = i // DB_BATCH_SIZE + 1
        if bn % 5 == 0 or bn == total_p_batches:
            log.info("    Primary batch %d/%d committed.", bn, total_p_batches)

    # ─── Duplicate updates (CASE/WHEN for per-row values) ───
    log.info("  Setting %d duplicate records…", len(duplicate_updates))
    total_d_batches = (len(duplicate_updates) + DB_BATCH_SIZE - 1) // DB_BATCH_SIZE
    for i in range(0, len(duplicate_updates), DB_BATCH_SIZE):
        batch = duplicate_updates[i:i + DB_BATCH_SIZE]
        # batch items: (primary_id, score, ai_decision, id)
        ids = [b[3] for b in batch]
        placeholders = ",".join(["%s"] * len(ids))

        # Build CASE expressions
        dup_of_cases = " ".join(f"WHEN {b[3]} THEN %s" for b in batch)
        score_cases = " ".join(f"WHEN {b[3]} THEN %s" for b in batch)
        ai_cases = " ".join(f"WHEN {b[3]} THEN %s" for b in batch)

        params = []
        params.extend(b[0] for b in batch)   # duplicate_of values
        params.extend(b[1] for b in batch)   # score values
        params.extend(b[2] for b in batch)   # ai_decision values
        params.extend(ids)                    # WHERE IN ids

        sql = (
            f"UPDATE masters SET "
            f"duplicate_status = 'duplicate', "
            f"duplicate_of = CASE id {dup_of_cases} END, "
            f"duplicate_score = CASE id {score_cases} END, "
            f"ai_decision = CASE id {ai_cases} END, "
            f"record_type = 'old' "
            f"WHERE id IN ({placeholders})"
        )
        cur.execute(sql, params)
        conn.commit()
        bn = i // DB_BATCH_SIZE + 1
        if bn % 5 == 0 or bn == total_d_batches:
            log.info("    Duplicate batch %d/%d committed.", bn, total_d_batches)

    # ─── Probable updates (CASE/WHEN) ───
    log.info("  Setting %d probable records…", len(probable_updates))
    total_pr_batches = max((len(probable_updates) + DB_BATCH_SIZE - 1) // DB_BATCH_SIZE, 1)
    for i in range(0, len(probable_updates), DB_BATCH_SIZE):
        batch = probable_updates[i:i + DB_BATCH_SIZE]
        # batch items: (score, ai_decision, id)
        ids = [b[2] for b in batch]
        placeholders = ",".join(["%s"] * len(ids))

        score_cases = " ".join(f"WHEN {b[2]} THEN %s" for b in batch)
        ai_cases = " ".join(f"WHEN {b[2]} THEN %s" for b in batch)

        params = []
        params.extend(b[0] for b in batch)  # score values
        params.extend(b[1] for b in batch)  # ai_decision values
        params.extend(ids)                   # WHERE IN ids

        sql = (
            f"UPDATE masters SET "
            f"duplicate_status = 'probable', "
            f"duplicate_score = CASE id {score_cases} END, "
            f"ai_decision = CASE id {ai_cases} END, "
            f"record_type = 'old' "
            f"WHERE id IN ({placeholders})"
        )
        cur.execute(sql, params)
        conn.commit()

    # ─── Insert new primary records where needed ───
    log.info("  Creating %d new primary records…", len(new_inserts))
    skipped = 0
    # Reconnect fresh — previous bulk updates may have exhausted the connection
    try:
        conn.ping(reconnect=True)
    except Exception:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
    for gi, ginfo in enumerate(new_inserts):
        # Keep connection alive
        if gi % 50 == 0 and gi > 0:
            try:
                conn.ping(reconnect=True)
            except Exception:
                conn = mysql.connector.connect(**DB_CONFIG)
                cur = conn.cursor()
            log.info("    New-record batch %d/%d…", gi, len(new_inserts))
        try:
            cur.execute(
                "INSERT INTO masters (type, label, duplicate_status, record_type) "
                "VALUES ('Company', %s, 'primary', 'new')",
                (ginfo["new_record_label"],),
            )
            new_primary_id = cur.lastrowid
        except mysql.connector.errors.IntegrityError:
            # Unique constraint — label already exists; use existing record
            conn.rollback()
            cur.execute(
                "SELECT id FROM masters WHERE type = 'Company' AND label = %s LIMIT 1",
                (ginfo["new_record_label"],),
            )
            row = cur.fetchone()
            if row:
                new_primary_id = row[0]
                cur.execute(
                    "UPDATE masters SET duplicate_status = 'primary', record_type = 'old' "
                    "WHERE id = %s", (new_primary_id,),
                )
            else:
                skipped += 1
                continue
        # Point all group members to the new primary
        for m in ginfo["members"]:
            cur.execute(
                "UPDATE masters SET duplicate_status = 'duplicate', "
                "duplicate_of = %s, duplicate_score = %s, ai_decision = %s, "
                "record_type = 'old' WHERE id = %s",
                (new_primary_id, m["score"], m["ai_decision"], m["id"]),
            )
        conn.commit()
    if skipped:
        log.info("  Skipped %d inserts (constraint + not found).", skipped)

    conn.commit()
    log.info("Database updates committed.")

    # ── Generate sample output ───────────────────────────────────
    log.info("Generating sample output…")
    output_lines = []
    output_lines.append("=" * 70)
    output_lines.append("DUPLICATE GROUP REPORT")
    output_lines.append(f"Total groups found: {len(group_info)}")
    output_lines.append("=" * 70)

    # Split into AI-validated (has member with ai_decision) and auto-confirmed
    def _quality_key(g):
        return (min((m["score"] for m in g["members"]), default=0), len(g["members"]))

    ai_validated = [g for g in group_info if any(m.get("ai_decision") for m in g["members"])]
    auto_confirmed = [g for g in group_info if g not in ai_validated]

    # Show AI-validated groups first (up to 5), then auto-confirmed (up to 10)
    ai_sorted = sorted(ai_validated, key=_quality_key, reverse=True)
    auto_sorted = sorted(auto_confirmed, key=_quality_key, reverse=True)
    groups_to_show = ai_sorted[:5] + auto_sorted[:10]
    if len(groups_to_show) < 15:
        remaining = [g for g in group_info if g not in groups_to_show]
        groups_to_show += sorted(remaining, key=_quality_key, reverse=True)[: 15 - len(groups_to_show)]

    output_lines.append(f"\n(AI-validated groups: {len(ai_validated)} | Auto-confirmed: {len(auto_confirmed)})")
    for idx, ginfo in enumerate(groups_to_show[:15], 1):
        is_ai = ginfo in ai_validated
        output_lines.append(f"\nGroup {idx}{' (AI-validated)' if is_ai else ''}:")
        if ginfo.get("new_record_label"):
            output_lines.append(
                f"  Primary (NEW): Label=\"{ginfo['new_record_label']}\" "
                f"[created as cleaned record]"
            )
            output_lines.append(
                f"  Original best: ID={ginfo['primary_id']}, "
                f"Label=\"{ginfo['primary_label']}\""
            )
        else:
            output_lines.append(
                f"  Primary: ID={ginfo['primary_id']}, "
                f"Label=\"{ginfo['primary_label']}\""
            )
        for m in ginfo["members"]:
            ai_str = ""
            if m["ai_decision"]:
                ai_str = f", AI={m['ai_decision']}"
            output_lines.append(
                f"  Duplicate: ID={m['id']}, Label=\"{m['label']}\", "
                f"Score={m['score']:.3f}{ai_str}"
            )

    output_text = "\n".join(output_lines)
    print(output_text)

    with open("sample_output.txt", "w") as f:
        f.write(output_text)
    log.info("Sample output saved to sample_output.txt")

    # ── Summary stats ────────────────────────────────────────────
    log.info("Querying final stats…")
    cur.execute(
        "SELECT duplicate_status, COUNT(*) FROM masters "
        "WHERE type = 'Company' GROUP BY duplicate_status"
    )
    stats = cur.fetchall()
    log.info("Final status distribution:")
    for status, count in stats:
        log.info("  %s: %d", status or "NULL (not in any group)", count)

    conn.close()
    elapsed = time.time() - t_start
    log.info("=== Done in %.1f seconds (%.1f min) ===", elapsed, elapsed / 60)


if __name__ == "__main__":
    main()

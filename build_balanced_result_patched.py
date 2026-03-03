#!/usr/bin/env python3
"""Build a patched copy of balanced_result for joined-vs-split token misses.

What it does:
  1) Recreates target table (default: balanced_result_patched) with the same
   schema as balanced_result.
  2) Copies all rows from balanced_result.
  3) Merges groups for cases like:
     "ICICI direct Securities"  <->  "ICICIdirect securities"
   by matching a compact distinctive signature.
  4) Resets and recomputes exactly one primary per valid group.

Usage:
  python3 build_balanced_result_patched.py

Optional env vars:
  SOURCE_TABLE=balanced_result
  TARGET_TABLE=balanced_result_patched
"""

import os
import re
import time
from collections import defaultdict

import mysql.connector
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

SOURCE_TABLE = os.getenv("SOURCE_TABLE", "balanced_result")
TARGET_TABLE = os.getenv("TARGET_TABLE", "balanced_result_patched")

STOPWORDS = {"the", "and", "of", "for", "in", "a", "an", "to", "at", "by", "on"}

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

_AMPERSAND_ACRONYM_RE = re.compile(r"\b([A-Za-z]{1,3})\s*&\s*([A-Za-z]{1,3})\b")

GENERIC_INDUSTRY_TERMS = {
  "group", "groups", "company", "companies", "corporation", "corporate",
  "enterprises", "enterprise", "industries", "industrial", "industry",
  "international", "global", "associates", "associate", "holding", "holdings",
  "limited", "private", "public", "division", "unit", "branch",
  "services", "service", "solutions", "solution", "management", "consulting",
  "advisory", "products", "product", "systems", "system", "operations", "operation",
  "financial", "finance", "bank", "banking", "insurance", "assurance",
  "engineering", "technology", "technologies", "software", "pharma",
  "pharmaceutical", "pharmaceuticals", "steel", "power", "energy", "motors",
  "oil", "gas", "chemicals", "textiles", "retail", "foods", "beverages",
  "logistics", "transport", "transportation", "construction", "infrastructure",
  "telecom", "telecommunications", "media", "digital", "manufacturing",
  "trading", "distribution", "stock", "investment", "investments", "securities",
  "capital", "credit", "lending", "leasing", "housing", "asset", "wealth",
  "consumer", "metals", "mining", "diagnostics", "biotech", "shipping",
  "freight", "courier", "airlines", "aviation", "garments", "apparel",
  "fashion", "realty", "properties", "property", "education", "training",
  "academy", "university", "college", "school", "india", "indian",
}


class UnionFind:
  def __init__(self):
    self.parent = {}
    self.rank = {}

  def find(self, value):
    if value not in self.parent:
      self.parent[value] = value
      self.rank[value] = 0
    if self.parent[value] != value:
      self.parent[value] = self.find(self.parent[value])
    return self.parent[value]

  def union(self, left, right):
    root_left = self.find(left)
    root_right = self.find(right)
    if root_left == root_right:
      return
    if self.rank[root_left] < self.rank[root_right]:
      root_left, root_right = root_right, root_left
    self.parent[root_right] = root_left
    if self.rank[root_left] == self.rank[root_right]:
      self.rank[root_left] += 1


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


def normalize(label: str) -> str:
  if not label:
    return ""
  value = label.lower().strip()
  value = _AMPERSAND_ACRONYM_RE.sub(lambda match: match.group(1) + match.group(2), value)
  value = value.replace("&", " and ")
  value = re.sub(r"[^\w\s]", " ", value)
  value = re.sub(r"\s+", " ", value).strip()
  value = LEGAL_SUFFIX_RE.sub("", value)
  value = re.sub(r"^the\s+", "", value)
  value = re.sub(r"\s+the$", "", value)
  value = re.sub(r"\s+", " ", value).strip()
  return value


def significant_tokens(norm: str):
  return [token for token in norm.split() if token not in STOPWORDS and len(token) > 1]


def distinctive_tokens(norm: str):
  tokens = significant_tokens(norm)
  return [token for token in tokens if token not in GENERIC_INDUSTRY_TERMS]


def compact_distinctive_signature(norm: str) -> str:
  distinctive = distinctive_tokens(norm)
  if not distinctive:
    return ""
  return "".join(distinctive)


def main():
  run_start = time.perf_counter()
  phase_start_time = run_start
  phase_name = "startup"

  def start_phase(name: str):
    nonlocal phase_start_time, phase_name
    now = time.perf_counter()
    elapsed = now - run_start
    print(f"\n[{elapsed:8.1f}s] ▶ {name}")
    phase_start_time = now
    phase_name = name

  def end_phase(extra: str = ""):
    now = time.perf_counter()
    phase_elapsed = now - phase_start_time
    total_elapsed = now - run_start
    suffix = f" — {extra}" if extra else ""
    print(f"[{total_elapsed:8.1f}s] ✓ {phase_name} ({phase_elapsed:.1f}s){suffix}")

  print("=" * 70)
  print("Building patched balanced table")
  print(f"  source: {SOURCE_TABLE}")
  print(f"  target: {TARGET_TABLE}")
  print("  patch: merge joined-vs-split token misses")
  print("=" * 70)

  conn = mysql.connector.connect(**DB_CONFIG)
  cur = conn.cursor()

  try:
    start_phase("Reset target table and copy source rows")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} LIKE {SOURCE_TABLE}")
    cur.execute(f"DELETE FROM {TARGET_TABLE}")
    cur.execute(
      f"""
      INSERT INTO {TARGET_TABLE}
        (company_name, original_name, master_id, source_type, group_id, is_primary, is_valid, filter_reason)
      SELECT
        company_name, original_name, master_id, source_type, group_id, is_primary, is_valid, filter_reason
      FROM {SOURCE_TABLE}
      """
    )
    conn.commit()
    end_phase("Copied source rows into target table")

    start_phase("Load valid rows")
    cur.execute(
      f"""
      SELECT id, company_name, source_type, group_id
      FROM {TARGET_TABLE}
      WHERE is_valid = 1 AND company_name IS NOT NULL AND TRIM(company_name) <> ''
      """
    )
    rows = cur.fetchall()
    end_phase(f"Loaded {len(rows):,} valid rows")

    start_phase("Build compact signatures")
    sig_to_rows = defaultdict(list)
    for row_id, company_name, source_type, group_id in rows:
      norm = normalize(company_name)
      d_tokens = distinctive_tokens(norm)
      signature = "".join(d_tokens)
      if len(signature) < 8:
        continue
      sig_to_rows[signature].append(
        (row_id, company_name, source_type, group_id, norm, len(d_tokens))
      )
    end_phase(f"Built {len(sig_to_rows):,} signature buckets")

    start_phase("Find group merge remaps")
    union_find = UnionFind()
    merge_bucket_count = 0
    merge_group_candidates = 0

    for _, members in sig_to_rows.items():
      if len(members) < 2:
        continue
      if len(members) > 25:
        continue

      has_split_form = any(member[5] >= 2 for member in members)
      has_joined_form = any(member[5] == 1 for member in members)
      if not (has_split_form and has_joined_form):
        continue

      groups = sorted({member[3] for member in members})
      if len(groups) < 2:
        continue
      merge_bucket_count += 1
      merge_group_candidates += len(groups)
      base_group = groups[0]
      for current_group in groups[1:]:
        union_find.union(base_group, current_group)

    if not union_find.parent:
      end_phase("No join/split-token missed merges detected")
      print("No join/split-token missed merges detected; target table is identical copy.")
      return

    components = defaultdict(list)
    for group_id in list(union_find.parent.keys()):
      components[union_find.find(group_id)].append(group_id)

    group_remap = {}
    for _, component_groups in components.items():
      target_group_id = min(component_groups)
      for old_group_id in component_groups:
        group_remap[old_group_id] = target_group_id

    update_pairs = [
      (new_group_id, old_group_id)
      for old_group_id, new_group_id in group_remap.items()
      if old_group_id != new_group_id
    ]
    end_phase(f"Prepared {len(update_pairs):,} group-id updates")

    start_phase("Apply group-id remaps")
    if update_pairs:
      pairs_old_new = [(old_gid, new_gid) for new_gid, old_gid in update_pairs]
      chunk_size = 250
      for idx in range(0, len(pairs_old_new), chunk_size):
        chunk = pairs_old_new[idx:idx + chunk_size]
        when_parts = [f"WHEN {old_gid} THEN {new_gid}" for old_gid, new_gid in chunk]
        in_values = ", ".join(str(old_gid) for old_gid, _ in chunk)
        update_sql = (
          f"UPDATE {TARGET_TABLE} "
          f"SET group_id = CASE group_id {' '.join(when_parts)} ELSE group_id END "
          f"WHERE is_valid = 1 AND group_id IN ({in_values})"
        )
        cur.execute(update_sql)
    end_phase(
      f"Merged {len(update_pairs):,} group ids across {merge_bucket_count:,} buckets"
    )

    print(f"    Candidate groups touched: {merge_group_candidates:,}")

    start_phase("Recompute primaries")
    cur.execute(f"UPDATE {TARGET_TABLE} SET is_primary=0")

    cur.execute(
      f"""
      UPDATE {TARGET_TABLE} t
      JOIN (
        SELECT id
        FROM (
          SELECT
            id,
            ROW_NUMBER() OVER (
              PARTITION BY group_id
              ORDER BY
                (
                  CASE source_type
                    WHEN 'Company' THEN 100
                    WHEN 'Companny' THEN 90
                    WHEN 'Group' THEN 50
                    WHEN 'Archived' THEN 10
                    ELSE 0
                  END
                  + CASE
                      WHEN company_name = UPPER(company_name) THEN -1
                      WHEN company_name = LOWER(company_name) THEN -1
                      WHEN LEFT(company_name, 1) REGEXP '[A-Z]' THEN 2
                      ELSE 0
                    END
                  + CASE
                      WHEN company_name REGEXP '(inc|ltd|llc|corp|plc|gmbh|pvt|limited|corporation|incorporated)' THEN 1
                      ELSE 0
                    END
                  + CASE
                      WHEN CHAR_LENGTH(TRIM(company_name)) >= 5
                      THEN LEAST(CHAR_LENGTH(TRIM(company_name)) / 30.0, 1.0)
                      ELSE 0
                    END
                  + CASE
                      WHEN company_name LIKE '%  %' OR company_name <> TRIM(company_name) THEN -1
                      ELSE 0
                    END
                ) DESC,
                CHAR_LENGTH(company_name) DESC,
                id ASC
            ) AS rn
          FROM {TARGET_TABLE}
          WHERE is_valid = 1
        ) ranked
        WHERE rn = 1
      ) p ON p.id = t.id
      SET t.is_primary = 1
      """
    )
    end_phase("Primary flags recomputed")

    start_phase("Finalize and summary")
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    total_rows = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(DISTINCT group_id) FROM {TARGET_TABLE} WHERE is_valid=1")
    total_groups = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_primary=1 AND is_valid=1")
    total_primaries = cur.fetchone()[0]

    print("Patch complete.")
    print(f"  rows:      {total_rows:,}")
    print(f"  groups:    {total_groups:,}")
    print(f"  primaries: {total_primaries:,}")
    if total_groups != total_primaries:
      print("WARNING: group/primary mismatch detected.")
    end_phase("Run completed")

  finally:
    cur.close()
    conn.close()


if __name__ == "__main__":
  main()
# Company Deduplication Workflow

End-to-end duplicate detection and classification for company records in the `masters` table of the `dedup_infollion` MySQL database.

## Summary

**Logic:** Normalize labels → block by prefix/token → score with RapidFuzz → auto-confirm ≥0.92, AI-validate 0.75–0.92 → Union-Find grouping → select primary → update DB.
**Run:** `pip install -r requirements.txt` → create `.env` with DB credentials → `python3 dedup.py`

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure credentials — create a .env file:
DB_HOST=172.105.61.195
DB_USER=intern
DB_PASSWORD=hWduyv324@#uqsdgtv!
DB_NAME=dedup_infollion
OPENROUTER_API_KEY=<your-key>          # only needed if USE_OPENROUTER=True
OPENROUTER_MODEL=meta-llama/llama-3.1-70b-instruct

# 3. Run
python3 dedup.py
```

The script is **idempotent** — it resets all dedup fields and removes any previously-created `record_type = 'new'` records before each run.

---

## How It Works

### Pipeline Overview

```
Load Company rows
  → Normalise labels
    → Blocking (prefix + token)
      → Score candidate pairs (RapidFuzz)
        → Threshold classification
          → AI validation of ambiguous pairs
            → Union-Find grouping
              → Select primary record per group
                → Update database
                  → Generate report
```

### Step 1 — Text Normalisation

Each company `label` is cleaned for comparison:

1. Lowercase + strip whitespace
2. Replace `&` → `and`
3. Remove all punctuation
4. Strip **core legal suffixes only** (Inc, LLC, Ltd, Corp, Pvt, Limited, GmbH, AG, SA, NV, LLP, Holdings, etc.) — *not* "global", "solutions", "services", which are often part of company names (e.g. "Global DB Solutions" stays distinct from "DB Corp")
5. Remove leading/trailing `the`
6. Collapse whitespace

Example: `"Tata Motor Finance Ltd."` → `"tata motor finance"`

### Step 2 — Blocking (Candidate Pair Generation)

With ~346K company records, comparing every pair (~60 billion) is infeasible. We use **multi-key blocking** to reduce candidates:

| Block Type | Key | Purpose |
|---|---|---|
| Prefix block | First 3 chars of normalised name | Catches similar-starting names |
| Token block | Alphabetically sorted non-stopword tokens | Catches reorderings & subsets |

Two records become a candidate pair if they share **any** block key. Blocks exceeding 500 members are skipped (too generic). **Single-token blocks** (e.g. "db", "hinduja") are also skipped to reduce false positives. This reduces the search space to ~20.5M pairs.

### Step 3 — Similarity Scoring

Each candidate pair receives a **weighted composite score** (0–1 scale) using [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz):

| Metric | Weight | Purpose |
|---|---|---|
| `token_sort_ratio` | 30% | Order-independent comparison |
| `token_set_ratio` | 30% | Handles subset/superset names |
| `ratio` | 20% | Raw Levenshtein similarity |
| `partial_ratio` | 20% | Handles one name inside another |

### Step 4 — Threshold Classification

| Score Range | Classification | Action |
|---|---|---|
| **≥ 0.920** | `duplicate` | Auto-confirmed — merged immediately |
| **0.750 – 0.919** | `probable` | Sent to AI for validation |
| **< 0.750** | — | Discarded (not a match) |

### Step 5 — AI Validation

Pairs scoring 0.75–0.919 are validated by an AI engine to decide if they are truly the same company.

**Two AI modes are available** (toggle `USE_OPENROUTER` in `dedup.py`):

| Mode | Setting | Description |
|---|---|---|
| **Local heuristic AI** (default) | `USE_OPENROUTER = False` | 6-rule engine: exact norm match, token containment, substring match, abbreviation/initials, high score + overlap, moderate score + overlap. Processes all ~417K pairs in ~2 seconds. |
| **OpenRouter / Llama 3.1 70B** | `USE_OPENROUTER = True` | Cloud LLM via OpenRouter API. Sends a structured prompt, parses JSON response. Capped at 200 calls (~$0.15 total). Rate-limited to 1 req/sec. |

**AI decision logic** (same for both modes):
- `same_company = true` and `confidence ≥ 0.7` → confirmed duplicate (merged)
- `same_company = true` and `confidence < 0.7` → stays as `probable`
- `same_company = false` → rejected (not merged)

The AI reasoning is stored in the `ai_decision` column as JSON.

### Step 6 — Union-Find Grouping

Confirmed duplicate pairs are merged into groups using a **Union-Find** (disjoint set) data structure with path compression and union-by-rank. This naturally handles **transitive relationships**: if A ≈ B and B ≈ C, then {A, B, C} form one group.

### Step 7 — Primary Record Selection

For each group, the **best existing record** is selected as primary using a quality scoring function:

| Factor | Score Impact |
|---|---|
| Title Case / proper capitalisation | +2 |
| ALL CAPS or all lowercase | −1 |
| Has legal suffix (Inc, Ltd, etc.) | +1 |
| Longer name (up to 30 chars) | +1 |
| Clean formatting (no double spaces, no junk chars) | +1 |
| Garbage pattern (test, unknown, -, n/a) | −5 |

**If the best record scores below the quality threshold** (`PRIMARY_QUALITY_MIN = 1`), a **new cleaned record** is inserted with:
- `duplicate_status = 'primary'`
- `record_type = 'new'`
- Label = title-cased version of the best existing label

All original records in the group are then marked as `duplicate` pointing to this new primary.

### Step 8 — Database Updates

Updates use **bulk CASE/WHEN SQL** for efficiency (single query per batch of 2000 rows):

- **Primary**: `duplicate_status = 'primary'`, `duplicate_of = NULL`
- **Duplicate**: `duplicate_status = 'duplicate'`, `duplicate_of = <primary_id>`, similarity score + AI reasoning stored
- **Probable**: `duplicate_status = 'probable'`, similarity score + AI reasoning stored
- All existing records: `record_type = 'old'`
- New cleaned records: `record_type = 'new'`

---

## Configuration

All tuneable parameters are constants at the top of `dedup.py`:

| Parameter | Default | Description |
|---|---|---|
| `USE_OPENROUTER` | `False` | Toggle between local AI and cloud LLM |
| `THRESHOLD_AUTO_DUPLICATE` | 0.92 | Score ≥ this → auto-confirmed |
| `THRESHOLD_PROBABLE` | 0.75 | Score ≥ this → send to AI |
| `AI_CONFIDENCE_ACCEPT` | 0.7 | AI confidence needed to confirm |
| `MAX_AI_CALLS` | 200 | Max OpenRouter API calls per run |
| `AI_CALL_DELAY` | 1.0 | Seconds between API calls |
| `MAX_BLOCK_SIZE` | 500 | Skip blocks larger than this |
| `PRIMARY_QUALITY_MIN` | 1 | Min score before creating new record |
| `DB_BATCH_SIZE` | 2000 | Rows per bulk SQL batch |

## Database Schema

The script adds these columns idempotently (skips if already present):

```sql
ALTER TABLE masters
  ADD COLUMN duplicate_status ENUM('primary','duplicate','probable') DEFAULT NULL,
  ADD COLUMN duplicate_of INT NULL,
  ADD COLUMN duplicate_score DECIMAL(5,3) NULL,
  ADD COLUMN ai_decision TEXT NULL,
  ADD COLUMN record_type ENUM('new','old') DEFAULT 'old';
```

| Column | Description |
|---|---|
| `duplicate_status` | Classification: `primary`, `duplicate`, or `probable` |
| `duplicate_of` | ID of the primary record (for duplicates) |
| `duplicate_score` | Fuzzy similarity score (0.000 – 1.000) |
| `ai_decision` | JSON with AI reasoning: `{same_company, confidence, reasoning}` |
| `record_type` | `'old'` for existing records, `'new'` for created primaries |

## Performance

| Phase | Duration | Notes |
|---|---|---|
| Load + normalise | ~10s | 345,976 Company records |
| Blocking | ~10s | 20.5M candidate pairs generated |
| Scoring | ~3 min | All 20.5M pairs scored |
| AI validation (local) | ~2s | 186,956 probable pairs evaluated |
| DB updates | ~1 min | Bulk CASE/WHEN SQL |
| **Total** | **~5 min** | vs 2.5+ hours with OpenRouter + individual UPDATEs |

## Output

The script generates `sample_output.txt` with 15 duplicate groups showing:
- Primary record (ID + label)
- All duplicates (ID + label + similarity score)
- AI reasoning (when applicable)

The report prioritises **AI-validated groups** (first 5) to demonstrate the full pipeline, followed by auto-confirmed groups.

## Project Structure

```
Assign/
├── dedup.py           # Main deduplication script (end-to-end)
├── requirements.txt   # Python dependencies
├── .env               # DB + API credentials (gitignored)
├── .gitignore
├── README.md          # This file
├── sample_output.txt  # Generated duplicate group report
├── explore_db.py      # Helper: initial DB exploration
└── explore_db2.py     # Helper: schema inspection
```

## Assumptions

1. Only rows with `type = 'Company'` are evaluated.
2. The `label` field is the sole comparison field.
3. Rows with empty or very short labels (< 2 chars) are excluded from matching.
4. The script resets all dedup columns before each run (idempotent).
5. AI is used only for ambiguous cases (score 0.75–0.919).
6. Transitive duplicates are grouped: if A ≈ B and B ≈ C, all three are in one group.
7. Records with garbage labels (e.g., `-`, `test`, `unknown`) get low primary scores and may trigger new record creation.

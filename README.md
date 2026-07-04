# Massive Music — Song & Video Metadata Pipeline

**Data Engineer Technical Test submission**

This repository contains the data pipeline that automates ingestion of song and video
metadata from Spotify and YouTube, matches it against Massive Music's internal 699-song
catalog, and produces a clean, query-ready dataset that answers two business questions:

1. How many YouTube videos does each song have?
2. How many Spotify ISRC recordings does each song have?

---

## Table of contents

1. [Project structure](#project-structure)
2. [Data pipeline architecture](#data-pipeline-architecture)
3. [Entity relationship diagram](#entity-relationship-diagram)
4. [Data storage layer design](#data-storage-layer-design)
5. [Data quality & validation strategy](#data-quality--validation-strategy)
6. [Pipeline monitoring & maintenance plan](#pipeline-monitoring--maintenance-plan)
7. [Known limitations](#known-limitations)
8. [How to run this pipeline](#how-to-run-this-pipeline)
9. [Security notes](#security-notes)
10. [Status & next steps](#status--next-steps)

---

## Project structure

```
.
├── ingest_massive_music_technical_test # Bronze: snapshot the source Excel catalog, api spotify, and api youtube
├── transform_massive_music_technical_test # Silver: parse, dedupe, validate, aggregate
└── README.md
```

---

## Data pipeline architecture

### Target production architecture (AWS-native)

The pipeline follows a **medallion architecture** (bronze → silver → gold), designed to
run natively on AWS:

```
Sources                Bronze (S3)              Silver (S3 + Glue)         Gold
─────────              ───────────              ──────────────────         ────────────────
Spotify API   ─┐                                                            
YouTube API   ─┼──▶    S3 bronze bucket   ──▶   S3 silver bucket     ──▶   Redshift Serverless
Excel catalog ─┘       raw JSON,                Parquet/Delta,             or Athena
                        append-only,             registered in             (BI-ready,
                        partitioned by dt=        Glue Data Catalog         high-perf querying)
```

#### High Level Architecture
https://drive.google.com/file/d/1jX5v-3jCEcclovqdxZrz2kuTKqhP70_N/view?usp=sharing

**Orchestration & monitoring layer** (runs alongside the data flow, not shown as a
separate data path):

- **EventBridge** — schedules pipeline runs (e.g. daily incremental catch-up for
  YouTube once quota resets, or on-demand for backfills)
- **Step Functions** — orchestrates the sequence: `ingest_song_catalog` →
  `ingest_api_spotify` + `ingest_api_youtube` (parallel) → `transform_bronze_to_silver`,
  with retry policies per state
- **CloudWatch** — collects logs and metrics from every Glue job / Lambda invocation;
  alarms on job failure or on abnormal duration
- **SNS** — notifies on alarm (e.g. Slack/email when a Glue job fails or YouTube quota
  is exhausted)
- **Secrets Manager** — stores Spotify `client_id`/`client_secret` and the YouTube
  `api_key`; nothing is ever hardcoded in code
- **DynamoDB** — per-`(source, song_id)` ingestion checkpoint table (`status`: DONE /
  FAILED, `attempts`, `error`, `updated_at`). This is what makes re-runs idempotent —
  a failed or interrupted run picks up exactly where it left off instead of
  re-processing already-completed songs (and re-spending API quota unnecessarily).

**Why bronze is append-only and immutable:** raw API responses and the raw catalog
snapshot are landed exactly as received, with no transformation. This means the
matching/parsing logic can be corrected and *replayed* against already-collected raw
data at any time, without re-hitting the Spotify/YouTube APIs — which matters a lot
given how rate-limited and quota-constrained both of those APIs turned out to be
during this build.

### Current implementation (development)

For this technical test, the pipeline runs on **Databricks notebooks + Delta Lake**
rather than deployed AWS infrastructure — no AWS account/billing was available for this
exercise, and Databricks Delta tables were used as a directly analogous stand-in:

| Target (AWS) | Current (Databricks) |
|---|---|
| S3 bronze bucket | `workspace.bronze.*` Delta tables |
| S3 silver bucket + Glue Catalog | `workspace.silver.*` Delta tables |
| Secrets Manager | Databricks secret scope (`dbutils.secrets.get(...)`) |
| Glue ETL job | `transform_bronze_to_silver` notebook (PySpark) |
| Step Functions / EventBridge | Manual/scheduled notebook runs (not yet automated) |

The table shapes, column names, and matching/dedup logic are written to be portable —
migrating to real AWS services later is a matter of swapping the I/O layer (Delta
table reads/writes → S3 + Glue Catalog reads/writes), not rewriting the transformation
logic itself.

---

## Entity relationship diagram

```
dim_song (1) ──────< fact_song_isrc (N)              dim_song (1) ── song_isrc_count (1)
  song_id (PK)         song_id (FK)                    song_id (FK)
  original_artist       isrc                            isrc_count
  song_title            recording_title
                        artist_name
                        album / release_date
                        match_confidence

dim_song (1) ──────< fact_song_youtube_video (N)     dim_song (1) ── song_video_count (1)
  song_id (PK)          song_id (FK)                    song_id (FK)
                        video_id                         video_count
                        channel_id / channel_title
                        video_title
                        published_at
```

- **`dim_song`** — one row per song from the internal catalog (699 rows). This is the
  master table; everything else joins back to it by `song_id`.
- **`fact_song_isrc`** — one row per (song, ISRC recording). One-to-many: a song can
  have multiple legitimate recordings (different artists, covers, acoustic versions —
  see the catalog's own "Andaikan kau datang kembali" example: 1 song → 4 recordings →
  4 ISRCs, by four different artists/labels). `match_confidence` (`high`/`low`) flags
  whether the match came from a precise `track+artist` query or a looser `title-only`
  fallback.
- **`fact_song_youtube_video`** — one row per (song, video). Same one-to-many
  reasoning.
- **`song_isrc_count`** / **`song_video_count`** — one row per song, one-to-one with
  `dim_song`. These are the tables that directly answer the two business questions;
  every `song_id` from `dim_song` appears here even if its count is `0`, so a failed
  match never silently disappears from a report.

---

## Data storage layer design

| Business requirement | Why this storage choice |
|---|---|
| Fast query: "how many ISRCs per song" | Pre-aggregated in `song_isrc_count` — no need to scan/parse raw JSON at query time |
| Analysts/data scientists need standard SQL access | Delta tables (dev) / Athena or Redshift (prod) are both queryable with plain SQL; no one needs to understand the Spotify/YouTube JSON response shape |
| Raw data must be re-processable if matching logic changes | Bronze is stored separately from silver, in its original raw form — reprocessing means re-running the transform, not re-calling the APIs |
| Future scalability to new data sources | Medallion architecture is additive: a new source becomes a new bronze table + a new silver transform step, without touching existing tables |
| High-performance querying/reporting for BI | Silver's Parquet/Delta format + Glue Catalog registration (prod) or Redshift/Athena (gold, prod) are built for this — columnar storage, predicate pushdown, no row-by-row JSON parsing on every query |

**Why not go straight to a relational database from the API responses?** Landing raw
JSON in bronze first, rather than parsing straight into a normalized DB, preserves the
option to change the parsing/matching logic later without needing to re-fetch data from
rate-limited, quota-constrained external APIs. This turned out to matter in practice —
see [Known limitations](#known-limitations).

---

## Data quality & validation strategy

These are real issues found and fixed during this build, not a theoretical checklist.

### 1. Source integrity (Excel catalog)

- ~49% of rows (342/699) have a missing `original_artist`.
- **Bug found & fixed:** on pandas 3.0, `df.where(pd.notnull(df), None)` was observed to
  write the literal string `"nan"` into missing cells instead of a real `None`/null —
  which would have made `bool(original_artist)` evaluate `True` for a "missing" value
  and silently broken the search-query branching logic downstream. Fixed by checking
  `pd.isna()` per value directly instead of using `.where()` across the whole frame.
- The catalog snapshot is treated as **immutable and append-only** in bronze
  (`snapshot_date` partition) — if the source Excel is corrected later, the old
  snapshot is retained for audit, not silently overwritten.

### 2. Matching strategy & confidence scoring (Spotify)

- Free-text search (`f"{title} {artist}"`) is ambiguous, especially for the 342 songs
  with no artist to disambiguate with.
- **Fallback query strategy**, tried in order until one returns results:
  1. `track:"{title}" artist:"{artist}"` — most precise, field-scoped
  2. `track:"{title}"` — field-scoped, title only
  3. `"{title} {artist}"` — free text, last resort
- Each match is tagged `match_confidence = high` (came from a query that included the
  artist) or `low` (title-only or free-text fallback), so downstream consumers know
  which rows deserve more scrutiny.
- **Real example found in the data:** `song_id 13809` ("BERTAHAN HIDUP" by 8 BALL,
  artist missing from this test's catalog row) returned 5 Spotify matches, only 1 of
  which was actually the same song — the other 4 were unrelated songs that matched
  purely on the artist name "8 Ball" appearing somewhere in the query. This is a
  concrete illustration of why title-only search is a real API limitation, not a
  theoretical risk.
- **Mitigation implemented:** the silver transform now joins `fact_song_isrc` back to
  `dim_song` and keeps only recordings whose title exactly matches the catalog's
  `song_title` (after stripping a trailing parenthetical like `(ACOUSTIC)` or `(LIVE)`,
  so legitimate alternate versions of the same song still pass). This is an
  application-side control compensating for a search API that cannot disambiguate
  by title alone when no artist signal is available.

### 3. Completeness (zero-match handling)

- Songs that fail to match are `LEFT JOIN`ed against the full catalog and `fillna(0)`,
  so they appear in `song_isrc_count` / `song_video_count` with a count of `0` rather
  than silently disappearing from the aggregate report. In the current Spotify data,
  3 songs have zero matches and are flagged as a manual-review queue.

### 4. Deduplication (fan-out ISRC/video)

- Each Spotify/YouTube search result item is exploded into its own row (not just
  `items[0]`), then deduplicated by `(song_id, isrc)` and `(song_id, video_id)` — this
  preserves legitimate multi-recording songs while still being safe to re-run without
  producing duplicate rows for the same recording.

### 5. Idempotency & failure isolation

- Per-`(source, song_id)` checkpoint in `ingestion_state` (`DONE` / `FAILED`, with
  `attempts` and `error` message).
- A failure on one song (`try/except` per iteration) never stops the rest of the batch.
- Re-running an ingestion notebook skips everything already marked `DONE` — safe to
  re-run after an interruption (rate limit, quota exhaustion, network failure) without
  wasting API calls on songs that already succeeded.

---

## Pipeline monitoring & maintenance plan

### Monitoring

| What to monitor | How (production, AWS) | How (current, Databricks) |
|---|---|---|
| Job success/failure | CloudWatch Logs + Step Functions execution status | Notebook run history + printed summary logs (`processed`, `matched`, `skipped`) |
| Per-song ingestion failures | Query `ingestion_state` where `status = 'FAILED'` | Same — `SELECT * FROM workspace.bronze.ingestion_state WHERE status = 'FAILED'` |
| API quota/rate-limit exhaustion | CloudWatch alarm on repeated 429/403 responses; SNS notification | Printed warning in notebook output (`QuotaExceeded` exception), manually observed |
| Data freshness | `MAX(snapshot_date)` / `MAX(ingested_at)` per bronze table, alarmed if stale beyond expected cadence | Same query, checked manually |
| Match quality drift | Track ratio of `match_confidence = 'low'` over time; alert if the proportion of low-confidence matches increases significantly | Not yet automated — currently a one-off check (`GROUP BY matched_query_strategy` / `queried_with_artist`) |
| Row-count sanity | Compare `dim_song` count against `song_isrc_count` / `song_video_count` counts — should always match 1:1 | Manual `SELECT COUNT(*)` comparison |

**Alerting thresholds (proposed for production):**
- Any Glue job failure → immediate SNS/Slack alert
- YouTube quota exhausted before completing the full catalog → alert (expected on a
  single API key; not itself an error, but should be visible so ingestion can resume
  the following day)
- Zero-match rate above a set threshold (e.g. >5% of songs) → alert, since this could
  indicate the catalog data or the search query logic has degraded

### Maintenance

- **Checkpoint table cleanup:** `ingestion_state` grows by one row per `(source,
  song_id)` — bounded by catalog size, no unbounded growth risk. No cleanup job
  needed unless songs are permanently removed from the catalog.
- **Bronze retention:** raw JSON in bronze is cheap to store (S3 Standard/Standard-IA)
  and valuable for reprocessing — recommend no automatic deletion, only lifecycle
  transition to cheaper storage classes after e.g. 90 days.
- **Credential rotation:** Spotify/YouTube credentials in Secrets Manager (prod) or
  Databricks secret scope (current) should be rotated periodically per standard
  practice; rotation requires no pipeline code changes since credentials are never
  hardcoded.
- **Schema evolution:** if Spotify or YouTube change their response shape, the
  `from_json` schema definitions in `transform_bronze_to_silver.py` need updating —
  since raw JSON is preserved in bronze, historical data can be re-parsed against a
  corrected schema without re-calling the APIs.
- **Re-running after quota reset:** `ingest_api_youtube` is safe to re-run daily (or
  via the EventBridge schedule in prod) until `ingestion_state` shows all songs
  `DONE` for the `youtube` source — no manual bookkeeping required.

---

## Known limitations

Documented as engineering trade-offs and external constraints, not hidden gaps.

| Limitation | Impact | Status |
|---|---|---|
| Spotify search `limit=5` per query | 85% of songs (596/699) are capped at exactly 5 matches — a lower bound, not an exhaustive count | Pagination (`offset` parameter) planned but not yet implemented |
| YouTube quota (10,000 units/day, 100 units/search) | Full 699-song ingestion not completed in a single day | External constraint; documented, not a pipeline defect |
| Spotify rate limiting (HTTP 429) | Some runs slowed by retry/backoff | Retry with `Retry-After` header handling implemented |
| `Label` field not captured | Spotify's `/v1/search` embedded album object omits `label` — requires a separate `GET /v1/albums/{id}` call per unique album | Deferred; batchable via `GET /v1/albums?ids=...` (up to 20 per call) as a follow-up step |
| Songwriter/composer data unavailable | Not exposed by Spotify's public API on any endpoint | Out of scope for this API; would require an internal publishing data source or a third-party service (e.g. MusicBrainz) |
| Title-only search collisions | Songs with no `original_artist` can match unrelated songs sharing an artist name (see `song_id 13809` example above) | Mitigated with a post-match title-equality filter against the catalog; not a perfect disambiguation, but removes the clear false positives observed |

---

## How to run this pipeline

**Prerequisites:** a Databricks workspace with a cluster running, and a secret scope
named `massive-music` containing `spotify-client-id`, `spotify-client-secret`, and
`youtube-api-key`.

```bash
# One-time: create the secret scope and populate it
databricks secrets create-scope massive-music
databricks secrets put-secret massive-music spotify-client-id
databricks secrets put-secret massive-music spotify-client-secret
databricks secrets put-secret massive-music youtube-api-key
```

Import all `.py` files in this repo into a Databricks workspace folder (they're
Databricks source-format notebooks — they'll render as notebooks with separated cells
automatically), then run in this order:

1. `check_api_connection` — verifies both APIs respond before committing to a full run
2. `ingest_song_catalog` — snapshots the source Excel into bronze
3. `ingest_api_spotify` — searches Spotify for every song, lands raw results
4. `ingest_api_youtube` — searches YouTube for every song, lands raw results (safe to
   re-run daily until complete, if quota-limited)
5. `transform_bronze_to_silver` — parses, filters, dedupes, and aggregates into the
   silver tables described above

All ingestion notebooks are idempotent — re-running any of them skips songs already
marked `DONE` in `ingestion_state`.

---

## Security notes

- No API credentials are hardcoded anywhere in this repository — all credentials are
  read at runtime via `dbutils.secrets.get(...)`.
- If you're reviewing an earlier version of any notebook and see a literal
  `client_id`/`client_secret`/`api_key` value, treat it as compromised and rotate it —
  it should not be present in the version submitted here.

---

## Status & next steps

| Item | Status |
|---|---|
| Spotify ingestion | Complete — 699/699 songs processed |
| YouTube ingestion | In progress — blocked by daily quota, resumes automatically on re-run |
| Bronze → silver transform | Complete, including title-match validation filter |
| Data Pipeline Architecture | Complete (this document + diagram) |
| Entity Relationship Diagram | Complete (this document) |
| Data Quality & Validation Strategy | Complete (this document) |
| Data Storage Layer Design | Complete (this document) |
| Pipeline Monitoring & Maintenance Plan | Complete (this document) |
| Presentation (PPTX) | Complete, summarizing the above for a non-technical review |
| Github submission | This repository |

**Immediate next steps:**
1. Complete YouTube ingestion once the daily quota resets.
2. Implement Spotify pagination (`offset`) for the 596 songs currently capped at
   `limit=5`, to get an exhaustive rather than lower-bound ISRC count.
3. Consider the `Label` batch-fetch step if the business requires it.
4. Automate orchestration (Step Functions + EventBridge) if/when this pipeline moves
   to real AWS infrastructure.

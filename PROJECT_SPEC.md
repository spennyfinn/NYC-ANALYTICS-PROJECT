# Project A: Modern Analytics Engineering Platform

> **Purpose of this document:** This is a complete, self-contained specification for building an end-to-end analytics engineering platform. It is written to be handed to an AI coding assistant (or followed by a developer) with no additional context required. Follow the milestones in order. Each milestone has explicit deliverables and acceptance criteria.

---

## 1. Project Summary

Build an end-to-end analytics platform that:
1. Ingests public data from an API into a cloud data warehouse (BigQuery).
2. Orchestrates the ingestion and transformation on a schedule (Dagster).
3. Transforms raw data into a clean dimensional model (dbt), including a Type 2 slowly changing dimension.
4. Tests and documents the data models (dbt tests + dbt docs).
5. Surfaces insights through a live BI dashboard (Looker Studio or Tableau Public).
6. Runs CI on pull requests (GitHub Actions).

**Resume one-liner this produces:** "Built an end-to-end analytics platform ingesting public data into BigQuery, orchestrated with Dagster, transformed with dbt into a dimensional model with SCD2 history, tested and documented, and surfaced through a live dashboard."

---

## 2. Tech Stack (use these exact tools)

| Layer | Tool | Notes |
|---|---|---|
| Language | Python 3.11+ | For ingestion scripts |
| Warehouse | Google BigQuery | Free tier: 10GB storage, 1TB queries/month |
| Orchestration | Dagster | Modern, Python-native, beginner-friendly |
| Transformation | dbt-core + dbt-bigquery | The core analytics engineering skill |
| BI / Dashboard | Looker Studio (free) | Native BigQuery connector; Tableau Public is an alternative |
| Version control | Git + GitHub | Public repo for portfolio |
| CI | GitHub Actions | Run `dbt build` against a CI dataset on PRs |
| Env management | `uv` or `venv` + `requirements.txt` | Pin versions |

---

## 3. Data Source

**Primary choice: NYC 311 Service Requests** via the Socrata Open Data API.
- Endpoint: `https://data.cityofnewyork.us/resource/erm2-nwe9.json`
- Docs: https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9
- Why: rich timestamps, geography (borough, zip, lat/long), categorical fields (agency, complaint type), and status fields that change over time, which makes for a natural fact + dimensions model and a believable SCD2.
- Use the `$where` filter on `created_date` and `$limit` / `$offset` for paginated incremental pulls.
- A Socrata app token is free and raises rate limits (register at the NYC Open Data portal). Store it in an environment variable.

**Alternatives if desired:** NYC Taxi trips, or EPA Air Quality (AQS API) for an environmental angle.

---

## 4. Prerequisites & Account Setup

Before any code, complete these one-time setup steps:

1. **Google Cloud account**
   - Create a GCP project (e.g. `nyc-analytics-platform`).
   - Enable the BigQuery API.
   - Create a **service account** with roles: `BigQuery Data Editor`, `BigQuery Job User`.
   - Download the service account JSON key. Store it OUTSIDE the repo; reference via `GOOGLE_APPLICATION_CREDENTIALS` env var.
   - Create two BigQuery datasets: `raw` (landing zone) and `analytics` (dbt output). Optionally `analytics_ci` for CI.
2. **Socrata app token** (free) for the NYC API.
3. **Python environment** with pinned dependencies.
4. **GitHub repo** (public) initialized with a sensible `.gitignore` (ignore the GCP key, `.env`, `venv/`, `target/`, `dbt_packages/`, `logs/`).

**Secrets (never commit these):**
```
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/key.json
GCP_PROJECT_ID=your-project-id
SOCRATA_APP_TOKEN=your-token
```

---

## 5. Repository Structure

```
nyc-analytics-platform/
  ingestion/
    __init__.py
    socrata_client.py      # API client with pagination + retries
    load_to_bigquery.py    # writes raw records to BigQuery `raw` dataset
    config.py              # env var loading
  dagster_project/
    __init__.py
    definitions.py         # Dagster Definitions: assets, jobs, schedules
    assets.py              # ingestion asset + dbt assets
  dbt_project/
    dbt_project.yml
    profiles.yml.example   # template; real profiles.yml stays local
    packages.yml           # dbt_utils
    models/
      staging/
        _staging__sources.yml
        stg_311__requests.sql
      intermediate/
        int_requests_enriched.sql
      marts/
        _marts__models.yml      # tests + docs descriptions
        fct_service_requests.sql
        dim_date.sql
        dim_agency.sql
        dim_complaint_type.sql
        dim_location.sql
        dim_status_scd2.sql
    tests/
      assert_resolution_time_non_negative.sql
    macros/
  dashboards/
    README.md              # link to live dashboard + screenshots
    screenshots/
  .github/
    workflows/
      ci.yml               # dbt build on PR
  requirements.txt
  .gitignore
  .env.example
  README.md
```

---

## 6. Dimensional Model Design

**Grain of the fact table:** one row per 311 service request.

### Fact: `fct_service_requests`
- `service_request_key` (surrogate key)
- `unique_key` (natural key from source)
- Foreign keys: `date_key` (created date), `agency_key`, `complaint_type_key`, `location_key`
- Measures: `resolution_time_hours` (closed_date - created_date), `is_resolved` (boolean), `request_count` (always 1, for easy summing)

### Dimensions
- **`dim_date`**: standard date spine (date_key, full_date, year, quarter, month, month_name, day_of_week, is_weekend). Generate with `dbt_utils.date_spine`.
- **`dim_agency`**: agency_key, agency_acronym, agency_name.
- **`dim_complaint_type`**: complaint_type_key, complaint_type, descriptor, category (derived grouping).
- **`dim_location`**: location_key, borough, incident_zip, city, latitude, longitude.
- **`dim_status_scd2`** (Type 2 slowly changing dimension): tracks status changes per request over time.
  - Columns: `status_key`, `unique_key`, `status`, `valid_from`, `valid_to`, `is_current`.
  - Implement using dbt snapshots (`snapshots/` dir with `dbt snapshot`) OR a manual SCD2 build. Prefer **dbt snapshots** with `strategy='check'` on the `status` column, since that is the idiomatic dbt approach and a strong talking point.

### Modeling layers (dbt best practice)
1. **staging** (`stg_`): 1:1 with source, rename/cast/clean only. Materialized as views.
2. **intermediate** (`int_`): joins and business logic. Materialized as views or ephemeral.
3. **marts** (`fct_` / `dim_`): final dimensional tables. Materialized as tables.

---

## 7. Milestones

### Milestone 1 — Ingestion to BigQuery (Week 1)

**Tasks**
- Build `socrata_client.py`: paginated GET against the 311 endpoint, with retries/backoff and the app token header. Support an incremental `since` date param filtering on `created_date`.
- Build `load_to_bigquery.py`: load fetched records into `raw.service_requests_311`. Use the BigQuery Python client; write with a defined schema or autodetect, append mode with a load timestamp column `_loaded_at`.
- Add `config.py` to read env vars. Add `.env.example`.
- Pull an initial backfill (e.g. last 90 days, cap rows during dev with `$limit`).

**Acceptance criteria**
- Running `python -m ingestion.load_to_bigquery --since 2026-01-01` populates `raw.service_requests_311` in BigQuery.
- Re-running does not crash on duplicates (append is fine; dedup happens in dbt staging).
- Secrets are read from env, never hardcoded.

### Milestone 2 — dbt Models + Star Schema (Week 2)

**Tasks**
- Initialize dbt project targeting BigQuery `analytics` dataset.
- Add `dbt_utils` via `packages.yml`.
- Define the source in `_staging__sources.yml` pointing at `raw.service_requests_311`.
- Build `stg_311__requests.sql`: clean types, standardize casing, dedup on `unique_key` keeping latest `_loaded_at`.
- Build all `dim_` and `fct_` marts per Section 6.
- Add `not_null`, `unique`, and `relationships` tests in `_marts__models.yml`.
- Add one custom singular test (`assert_resolution_time_non_negative.sql`).

**Acceptance criteria**
- `dbt build` runs clean (all models + tests pass).
- The star schema exists in the `analytics` dataset: 1 fact + 4 dimensions.
- Fact-to-dimension `relationships` tests pass.

### Milestone 3 — SCD2 + Docs + Orchestration (Week 3)

**Tasks**
- Implement `dim_status_scd2` as a dbt snapshot (`strategy='check'`, `check_cols=['status']`). Document how it captures status history.
- Generate dbt docs (`dbt docs generate`) and capture the lineage graph screenshot for the README.
- Stand up Dagster: model the ingestion as an asset and wrap dbt with `dagster-dbt` so dbt models appear as downstream assets.
- Add a **daily schedule** that runs ingestion then `dbt build` then snapshot.

**Acceptance criteria**
- `dbt snapshot` produces an SCD2 table with `valid_from`, `valid_to`, `is_current`.
- `dagster dev` shows the full asset lineage (ingestion -> staging -> marts).
- A schedule is defined and can be triggered manually to run the whole pipeline.

### Milestone 4 — Dashboard + CI + README (Week 4)

**Tasks**
- Build a Looker Studio dashboard connected to BigQuery `analytics` with 3–5 KPIs:
  - Complaint volume over time (trend)
  - Average resolution time by borough
  - Top complaint types by agency
  - Open vs resolved breakdown
  - A map using lat/long (optional but impressive)
- Set the dashboard to public/viewable; capture screenshots into `dashboards/screenshots/`.
- Add GitHub Actions `ci.yml`: on PR, install dbt, run `dbt build` against an `analytics_ci` dataset using a CI service account (store key as a GitHub secret).
- Write the `README.md`: architecture diagram, stack, setup instructions, lineage screenshot, dashboard link + screenshots, and a short "Insights" section with 3 findings from the data.

**Acceptance criteria**
- A public dashboard link works and is embedded/linked in the README.
- CI runs `dbt build` automatically on a PR and passes.
- README is complete enough that a stranger could reproduce the project.

---

## 8. Definition of Done (whole project)

- [ ] Ingestion script pulls NYC 311 data into BigQuery `raw`, incrementally by date.
- [ ] dbt project transforms raw into a documented, tested star schema in `analytics`.
- [ ] A Type 2 SCD (dbt snapshot) tracks request status history.
- [ ] Dagster orchestrates the full pipeline on a daily schedule.
- [ ] dbt docs lineage graph is generated and screenshotted.
- [ ] A public Looker Studio dashboard presents 3–5 KPIs.
- [ ] GitHub Actions runs `dbt build` on PRs.
- [ ] README documents architecture, setup, lineage, dashboard, and insights.
- [ ] No secrets are committed; `.env.example` documents required variables.

---

## 9. Stretch Goals (optional, only after Definition of Done)

- Add data freshness checks (`dbt source freshness`).
- Add `dbt-expectations` for richer tests.
- Incremental materialization on the fact table instead of full refresh.
- Add a simple data quality alert (Dagster sensor on test failures).
- Containerize the ingestion + Dagster with Docker.

---

## 10. Instructions for the AI Assistant Executing This

- Work milestone by milestone; do not skip ahead. Confirm each milestone's acceptance criteria before moving on.
- Pin all dependency versions in `requirements.txt` / `packages.yml`.
- Never hardcode secrets; always read from environment variables and document them in `.env.example`.
- Prefer idiomatic dbt structure (staging / intermediate / marts) and the official `dbt_utils` package.
- Keep all SQL readable and add `description:` fields in the YML so `dbt docs` are meaningful.
- After each milestone, output: what was built, how to run it, and how to verify the acceptance criteria.

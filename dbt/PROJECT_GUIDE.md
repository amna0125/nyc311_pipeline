# dbt Project Guide (plain-English)

A quick tour of what every part of this dbt project does and why. Brief on
purpose — ask me about any piece if you want more depth.

## The big picture

dbt takes the **raw NYC 311 table** that dlt loaded into Snowflake and turns it
into **clean, analytics-ready tables**, in three layers (the "medallion"):

```
raw (dlt)  →  BRONZE (mirror)  →  SILVER (clean)  →  GOLD (ready for dashboards)
```

Each layer is just SQL `SELECT` statements. dbt runs them in the right order,
creates the tables/views in Snowflake, and runs tests on the results.

---

## Top-level files

| File | What it does | Why |
|---|---|---|
| `dbt_project.yml` | The project's settings: where models live, and how each layer is built (bronze/silver = views, gold = tables). Also holds shared values like the NYC map boundaries. | Tells dbt the rules for the whole project so we don't repeat them in every file. |
| `packages.yml` | Lists 3 free add-on libraries (`dbt_utils`, `dbt_expectations`, `codegen`). | They give us ready-made helpers and tests so we don't write them from scratch. Installed with `dbt deps`. |
| `profiles.yml` | The Snowflake connection (account, user, database…). It reads the values from your `.env`. | dbt needs to know *where* to build. Reusing `.env` means one place for credentials. |
| `package-lock.yml` | Auto-generated; pins exact package versions. | Reproducible installs. You don't edit this. |

---

## The folders

### `models/` — the actual transformations
This is the heart of the project. Each `.sql` file = one table or view in
Snowflake. Organized into the three layers:

- **`models/bronze/`** — a faithful **mirror** of the raw table.
  - `stg_nyc311__service_requests.sql`: renames columns to clean names, fixes
    data types (text → dates/numbers), and removes duplicates. **No business
    logic** — just tidy the raw data.
  - *Why:* gives every downstream model one consistent, typed starting point.

- **`models/silver/`** — **clean and enrich**.
  - `int_service_requests__cleaned.sql`: replaces junk values (`"Unspecified"`,
    `"N/A"`) with NULL, standardizes things (e.g. borough → `MANHATTAN`), drops
    bad ZIPs/coordinates.
  - `int_service_requests__enriched.sql`: adds **derived columns** —
    `resolution_time_hours`, `is_closed`, `is_overdue`, date parts, etc.
  - *Why:* this is where messy real-world data becomes trustworthy and useful.

- **`models/gold/`** — **ready for analysis** (the "one big table" + marts).
  - `facts/fct_service_requests.sql`: one wide row per service request with all
    attributes + measures. This is the main table to query.
  - `dimensions/dim_date.sql`: a calendar table (one row per day, with
    weekend/month/quarter flags). The one dimension we keep separate because it
    enables "days with zero requests" and BI date features.
  - `marts/mart_*.sql`: small pre-summarized tables for dashboards (daily counts
    by borough/agency, SLA performance, open-vs-closed backlog).
  - *Why:* dashboards query these directly — fast and simple.

### `models/**/_*.yml` — the description + test files
Files like `_bronze__sources.yml`, `_silver__models.yml`, `_gold__facts.yml`.
They are **not** SQL — they **describe** the models and **attach tests** (e.g.
"this column must be unique", "borough must be one of these values").
- *Why:* this is how we guarantee data quality and document columns. The `_`
  prefix just keeps them sorted at the top of the folder.

### `seeds/` — small reference CSVs
Plain CSV files dbt loads into Snowflake as small tables:
- `seed_agency_codes.csv`, `seed_location_types.csv`: the lists of *allowed*
  values, used by tests to flag anything unexpected.
- `seed_expected_source_columns.csv`: the list of columns the raw table *should*
  have — used to detect if the API changes its schema.
- *Why:* reference lists that rarely change live better as version-controlled
  CSVs than hard-coded inside SQL. Loaded with `dbt seed`.

### `tests/` — custom checks
- `assert_source_schema_unchanged.sql`: compares the live raw table's columns to
  the expected list (the seed) and **warns if columns were added/renamed/removed**.
- *Why:* catches upstream API changes before they silently break the pipeline.
  (Most tests are the simple ones written in the `_*.yml` files; this folder is
  for checks that need real SQL.)

### `macros/` — reusable SQL snippets
- `clean_sentinel.sql`: a small reusable function that turns junk values
  (`""`, `"Unspecified"`, `"N/A"`…) into NULL.
- *Why:* we need that same cleanup on ~15 columns. Writing it once as a macro and
  calling it everywhere keeps the code short and consistent (see Jinja below).

### `target/` and `dbt_packages/` (auto-created, not in git)
- `dbt_packages/`: the downloaded add-on libraries.
- `target/`: dbt's compiled SQL and run results.
- *Why:* generated output — safe to ignore/delete; recreated by `dbt deps` / runs.

---

## Where we use Jinja, and why

**Jinja** is the `{{ ... }}` / `{% ... %}` templating language mixed into the SQL.
dbt uses it so our SQL can be dynamic instead of hard-coded. The main uses here:

| Jinja you'll see | Plain meaning | Why |
|---|---|---|
| `{{ ref('int_service_requests__cleaned') }}` | "the table built by this other model" | Lets dbt figure out the build order automatically and use the right schema name — never hard-code table names. |
| `{{ source('nyc_311', 'service_requests_dlt') }}` | "the raw dlt table" | Points models at the raw source in one declared place. |
| `{{ config(materialized='incremental', ...) }}` | per-model settings | E.g. tells the fact to update only new rows instead of rebuilding. |
| `{% if is_incremental() %} ... {% endif %}` | "only on incremental runs" | Adds the `WHERE ingested_at > ...` filter so we process just new/changed rows. |
| `{{ var('nyc_lat_min') }}` | a shared value from `dbt_project.yml` | Keeps the NYC map boundaries in one place. |
| `{{ dbt_utils.generate_surrogate_key(...) }}` / `{{ dbt_utils.date_spine(...) }}` | helpers from a package | Reuse battle-tested code (e.g. building the calendar) instead of writing it. |

## Where we use macros, and why

A **macro** is our *own* reusable Jinja function. We wrote one:
- `clean_sentinel('borough')` expands into a chunk of SQL that strips junk values
  to NULL. We call it on many columns in the silver layer.
- *Why:* DRY ("Don't Repeat Yourself") — fix the cleanup logic in one file and
  every column that uses it updates automatically.

(The package functions like `generate_surrogate_key` are also macros — just ones
that came from `dbt_utils` instead of being written by us.)

---

## How it all runs together

1. `dbt deps` → download the packages.
2. `dbt seed` → load the reference CSVs.
3. `dbt run` → build bronze → silver → gold (dbt orders them via `ref()`).
4. `dbt test` → run all the checks from the `_*.yml` files + `tests/` folder.

`dbt build` does steps 2–4 together, in dependency order. In your setup, the
Airflow `nyc_311_dbt_local` DAG runs `dbt deps` then `dbt build` automatically
after each dlt load.

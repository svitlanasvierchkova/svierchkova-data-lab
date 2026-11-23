# Store Analytics in Microsoft Fabric

## Status and prerequisites
This is a **project scaffold**, not a deployed or fully verified implementation. The assignment specifies 17 ShopDB tables but **does not provide their actual names, columns, keys, credentials, or the course server address**. Therefore the source-specific mappings and executable Fabric pipeline activity definitions cannot be completed reliably without inspecting the database and configuring Fabric connections. No pipeline has been run, and no screenshots or exported Fabric JSON are claimed.

## Architecture
PostgreSQL `shop.shop_db` (17 tables) + NBU API (USD/PLN) → `lh_bronze` (raw + metadata) → `lh_silver` (typed, deduplicated, Delta MERGE) → `lh_gold` (star schema + USD) → Direct Lake semantic model. Four pipelines: Bronze, Silver, Gold, and master sequential orchestration.

## Setup (click-by-click)
1. Sign into Microsoft Fabric, select **Workspaces → New workspace**; create `ws_shop_analytics` in Fabric-capacity-enabled workspace.
2. Inside the workspace select **New item → Lakehouse** three times: `lh_bronze`, `lh_silver`, `lh_gold`.
3. Create a PostgreSQL connection in **Manage connections and gateways**. Use read-only course credentials and keep passwords inside Fabric, not in source control. Verify network/gateway reachability.
4. Query `information_schema.tables` and `information_schema.columns` in PostgreSQL (see `sql/source_discovery.sql`). Document all 17 tables, primary keys, dates, currencies and relationships. Replace `config.example.json` placeholders with discovered mapping (do not commit credentials).
5. Create `etl_control` in Bronze using a Fabric Spark notebook and `sql/control_table.sql`. Seed one active row per real table.
6. Create `pl_bronze`: **Lookup** active control rows → **ForEach** sequential or controlled concurrency → **Copy data** PostgreSQL table to Bronze Delta. Use dynamic source/target table names; add source and ingestion timestamp metadata. For full loads, replace the raw snapshot; for incremental loads, filter on a high-water mark and append, with overlap and Silver deduplication. Update control state only after successful Copy and validate row counts. Ingest NBU USD and PLN JSON into raw Bronze separately.
7. Create a Silver notebook based on `notebooks/02_silver_merge.ipynb`, add one real business-key mapping per source table, type casts, normalization, and deduplication. Use Delta `MERGE` and verify repeat-run stability.
8. Create a Gold notebook using `notebooks/03_gold_template.ipynb`. Map real source joins to date/customer/product/geography/carrier/order-profile dimensions, fact_sales (order line grain), and fact_payments (payment/refund grain). Use stable surrogate-key strategy, MERGE, and integrity tests.
9. Create `pl_silver` and `pl_gold` using Notebook activities; then `pl_main` using three **Invoke pipeline** activities linked by **Succeeded** dependencies. Set retries on network-dependent activities, alerts/failure logging, and a daily trigger.
10. Create a semantic model from Gold tables in **Direct Lake** mode; define 1:* dimension-to-fact relationships and DAX measures from `docs/dax_measures.dax`. Check the model actually remains in Direct Lake mode.
11. Run `pl_main` twice on unchanged data; compare row counts, sums, and sampled USD conversions against NBU historical rates. Check orphan foreign keys = 0.
12. Export actual Fabric notebooks (`.ipynb`) and pipeline JSON from Fabric, save three required screenshots, and submit with README and `.gitignore`.

## Currency logic
`amount_usd = amount_local * currency_to_uah_rate / usd_to_uah_rate`. For UAH, currency_to_uah_rate = 1. For PLN, use the PLN rate on the transaction date. Reject or quarantine rows with missing/zero USD rates. Decide and document how to handle NBU non-publishing dates (e.g., carry forward the most recent published rate) before producing Gold. Use decimal arithmetic, not floating point, for money.

## Data-quality acceptance
- Bronze contains 17 source tables and raw NBU records; control-table toggles work without editing the pipeline.
- Silver/Gold MERGE is idempotent on unchanged input; counts and money totals stable after rerun.
- Fact keys resolve to existing dimension rows; no orphan references.
- USD conversions match independent manual NBU checks for both countries.
- Direct Lake model measures agree with Gold SQL.
- Master pipeline completes successfully; no credentials in Git.

## Files
- `notebooks/01_nbu_rates.ipynb`: NBU ingestion sample
- `notebooks/02_silver_merge.ipynb`: reusable Delta MERGE helper
- `notebooks/03_gold_template.ipynb`: star-schema mapping and currency helper
- `sql/source_discovery.sql`: discover source schema
- `sql/control_table.sql`: control table definition
- `sql/validation.sql`: sample reconciliation queries
- `docs/dax_measures.dax`: seven measures
- `pipelines/`: design specs; **not Fabric-exported executable pipeline JSON**

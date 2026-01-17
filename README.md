# Data Modeling Workshop on Databricks (Free Edition)

This repository demonstrates, end-to-end, the construction of a small Data Warehouse on Databricks using the Bronze → Silver → Gold pattern with Delta Lake, including SCD Type 2 for customers, incremental deduplication, fact/dimension tables, and analytical views.

The instructions and notebooks were designed to also run on Databricks Free Edition (Community/Trial), with notes about environments with/without Unity Catalog.

## Architecture and Flow

- **Bronze**: generation/simulation of heterogeneous raw data (types like string, null values, and inconsistencies) directly in Delta tables.
- **Silver**: incremental curation with `MERGE` and watermark windows. Standardizes types, normalizes status, deduplicates records, and maintains SCD Type 2 for customers.
- **Gold**: publication of the dimensional model (dimensions and facts), with current customer snapshot and complete SCD version, plus convenience views.

## Repository Structure

- **notebooks/**
  - `bronze_layer.ipynb`: generates Bronze tables (`orders`, `order_items`, `customers`, `products`).
  - `silver_layer.ipynb`: creates/updates Silver tables (`orders_clean`, `order_items_clean`, `products_clean`, `dim_customer_scd`).
  - `gold_layer.ipynb`: creates/updates Gold dimensions and facts (`dim_tempo`, `dim_produto`, `dim_cliente`, `dim_cliente_scd`, `fact_vendas`, `fact_vendas_current`) and views (`vw_vendas_diarias`, `vw_receita_por_estado`).

- **models/**
  - `silver/*.dbquery.ipynb` and `gold/*.dbquery.ipynb`: notebooks focused on specific SQL queries for the Silver/Gold layers.

- **jobs/**
  - `data_warehouse_job.yaml`: Job definition (Databricks Asset Bundle) with task dependencies based on SQL queries.

## Created Objects (tables and views)

- Bronze (schema: `bronze`)
  - `products`, `customers`, `orders`, `order_items`

- Silver (schema: `silver`)
  - `orders_clean` (dedupe + idempotent hash)
  - `order_items_clean` (dedupe by `order_id`,`product_id` + hash)
  - `products_clean` (normalized types + hash)
  - `dim_customer_scd` (SCD Type 2: `effective_start`, `effective_end`, `is_current`)

- Gold (schema: `gold`)
  - `dim_tempo` (insert-only with `tempo_sk`)
  - `dim_produto` (NK=`product_id`, generated SK)
  - `dim_cliente` (current snapshot from Silver SCD, 1 row per NK)
  - `dim_cliente_scd` (time-aware replica of Silver SCD)
  - `fact_vendas` (grain: order item, with FKs to time, product, and customer SCD)
  - `fact_vendas_current` (snapshot based on current `dim_cliente`)
  - Views: `vw_vendas_diarias`, `vw_receita_por_estado`

## How to Execute (Databricks)

1) **Import the notebooks** to your Databricks workspace:
- Via Databricks Repos (git) or manual upload of `notebooks/*.ipynb`.

2) **Create and start a cluster** (or use an existing one) with Delta Lake support.
- The Bronze notebook installs `Faker` via `%pip install Faker` in the first cell if needed.

3) **Define usage (with/without Unity Catalog)**:
- In the notebooks, the default uses the `workshop_modelagem` catalog and `bronze`, `silver`, `gold` schemas.
- If your workspace DOES NOT have Unity Catalog, adjust:
  - In `bronze_layer.ipynb`, set `CATALOG = None` (there's already support in the code) to create only databases (without catalog).
  - In the `silver_layer.ipynb` and `gold_layer.ipynb` notebooks, remove/ignore the lines `USE CATALOG workshop_modelagem;` and ensure that commands point to the simple schemas (`bronze`, `silver`, `gold`).

4) **Recommended execution order**:
- `notebooks/bronze_layer.ipynb`
- `notebooks/silver_layer.ipynb`
- `notebooks/gold_layer.ipynb`

5) **Quick validations** (examples):
```sql
-- Net revenue by day
SELECT * FROM gold.vw_vendas_diarias ORDER BY data;

-- Net revenue by day and state
SELECT * FROM gold.vw_receita_por_estado ORDER BY data, state;

-- Fact partitioned by time
SELECT COUNT(*) FROM gold.fact_vendas;
```

## Incremental and Idempotency

- Silver uses `MERGE INTO` with row hashes to avoid updates without actual changes.
- Deterministic deduplication by window (`ROW_NUMBER()`) in `orders` and `order_items`.
- Watermarks (e.g., 60–90 days) control incremental windows.
- Gold reuses hashes and business keys for idempotent `MERGE`.

## SCD Type 2 (Customers)

- Implemented in `silver.dim_customer_scd` with columns `effective_start`, `effective_end`, `is_current`, and `row_hash`.
- Published in Gold as:
  - `gold.dim_cliente` (current snapshot; 1 row per `nk_customer_id`).
  - `gold.dim_cliente_scd` (complete history for time-aware joins).

## Job (Databricks Asset Bundle)

- File: `jobs/data_warehouse_job.yaml`.
- Defines the "e-commerce data warehouse" job with tasks and dependencies in SQL (Databricks SQL Warehouse required):
  - Silver: `orders_clean`, `order_items_clean`, `products_clean`, `dim_customer_scd`.
  - Gold: `dim_tempo`, `dim_produto`, `dim_cliente_scd`, `dim_cliente`, `fact_vendas`.
- Important notes:
  - The `query_id` and `warehouse_id` fields are specific to your workspace. You will need to create the queries in the SQL editor (copying from the notebooks) and replace the IDs in the YAML, or orchestrate via notebooks in an all-purpose cluster.
  - In workspaces without Unity Catalog or without SQL Warehouse, execute via notebooks in the order indicated above.

## Troubleshooting

- **Error in `USE CATALOG`**: your workspace probably doesn't have Unity Catalog. Follow step 3 to disable the catalog (use only databases/schemas).
- **Permissions/Schemas**: ensure that you can create databases/schemas `bronze`, `silver`, `gold` (or adjust names according to your convention).
- **Reprocessing**: Bronze uses `overwrite` (regenerates synthetic data). Silver/Gold are idempotent and update only when there's a real change (different hash).

## License

Educational/workshop use. Adjust according to your organizational needs.

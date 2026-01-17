# Data Modeling & Medallion Architecture on Databricks

## Overview

This repository provides a comprehensive, end-to-end implementation of a **modern Data Warehouse** on Databricks, demonstrating industry best practices for data engineering and dimensional modeling. The project showcases the **Medallion Architecture** (Bronze → Silver → Gold) using Delta Lake, with advanced features including:

- ✨ **SCD Type 2** implementation for historical tracking
- 🔄 **Incremental processing** with idempotent operations
- 📊 **Dimensional modeling** (Star Schema)
- 🎯 **Data quality** controls and deduplication
- 🚀 **Production-ready patterns** for ETL/ELT pipelines

**Perfect for:** Data engineers learning Databricks, teams implementing medallion architecture, or anyone building their first dimensional data warehouse.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Understanding the Medallion Architecture](#understanding-the-medallion-architecture)
- [What is SCD Type 2?](#what-is-scd-type-2)
- [Repository Structure](#repository-structure)
- [Data Model](#data-model)
- [Getting Started](#getting-started)
- [Incremental Processing & Idempotency](#incremental-processing--idempotency)
- [Job Orchestration](#job-orchestration)
- [Query Examples](#query-examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MEDALLION ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐      ┌──────────────┐       ┌──────────────┐      │
│  │   BRONZE     │      │   SILVER     │       │    GOLD      │      │
│  │  (Raw Data)  │ ───▶ │  (Curated)   │ ───▶ │ (Business)   │      │
│  └──────────────┘      └──────────────┘       └──────────────┘      │
│                                                                     │
│  • Raw ingestion       • Data quality        • Dimensional model    │
│  • No transformation   • Deduplication       • Star schema          │
│  • Append/Overwrite    • Type casting        • Aggregations         │
│  • Mixed data types    • SCD Type 2          • Business views       │
│                        • Incremental MERGE   • Analytics ready      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

                    ▼ Consumed by ▼
        
        ┌────────────────────────────────┐
        │   Business Intelligence Tools  │
        │   • Power BI / Tableau         │
        │   • SQL Analytics              │
        │   • ML Models                  │
        └────────────────────────────────┘
```

---

## Understanding the Medallion Architecture

### **Bronze Layer** - Raw Zone
The landing zone for all source data, preserving the original format.

**Characteristics:**
- **Purpose**: Immutable, auditable raw data store
- **Format**: Delta tables with minimal transformation
- **Data Quality**: Accepts dirty data (nulls, type mismatches, duplicates)
- **Load Pattern**: Full overwrites or append-only
- **Retention**: Long-term storage of source data

**Tables in this project:**
- `bronze.customers` - Customer master data
- `bronze.products` - Product catalog
- `bronze.orders` - Order headers
- `bronze.order_items` - Order line items

### **Silver Layer** - Cleansed Zone
Validated, deduplicated, and enriched data ready for business logic.

**Characteristics:**
- **Purpose**: Clean, conformed data with business rules applied
- **Format**: Delta tables with enforced schemas
- **Data Quality**: Type validation, deduplication, null handling
- **Load Pattern**: Incremental MERGE operations (idempotent)
- **Change Tracking**: SCD Type 2 for slowly changing dimensions

**Transformations applied:**
- ✅ Data type normalization (e.g., strings → proper types)
- ✅ Deduplication using deterministic logic
- ✅ Status code standardization
- ✅ Row-level hashing for change detection
- ✅ Watermark-based incremental windows (60-90 days)

**Tables in this project:**
- `silver.orders_clean` - Validated orders with hash keys
- `silver.order_items_clean` - Deduplicated order items
- `silver.products_clean` - Type-safe product data
- `silver.dim_customer_scd` - **Customer dimension with full history (SCD Type 2)**

### **Gold Layer** - Business Zone
Dimensional model optimized for analytics and reporting.

**Characteristics:**
- **Purpose**: Business-ready data marts and aggregations
- **Format**: Star schema (facts + dimensions)
- **Data Quality**: Referential integrity enforced
- **Load Pattern**: Incremental updates based on Silver changes
- **Performance**: Optimized for query performance

**Objects in this project:**
- **Dimensions:**
  - `gold.dim_tempo` - Date dimension
  - `gold.dim_produto` - Product dimension (current state)
  - `gold.dim_cliente` - Customer dimension (current snapshot)
  - `gold.dim_cliente_scd` - Customer dimension (full history)
  
- **Facts:**
  - `gold.fact_vendas` - Sales fact (grain: order item, time-aware)
  - `gold.fact_vendas_current` - Sales fact (current customer state)
  
- **Views:**
  - `gold.vw_vendas_diarias` - Daily sales summary
  - `gold.vw_receita_por_estado` - Revenue by state

---

## What is SCD Type 2?

**Slowly Changing Dimension Type 2** is a dimensional modeling technique that **preserves the complete history** of changes to dimension attributes over time.

### Why Use SCD Type 2?

In real-world scenarios, dimension attributes change:
- Customer moves to a new state
- Product category is reclassified
- Employee gets promoted to a new department

**SCD Type 2 allows you to answer questions like:**
- "What was the customer's address when they placed order #1234?"
- "Show me sales by the customer's state at the time of purchase"
- "How many customers were in California in Q1 2023?"

### Implementation Strategy

Instead of **updating** (SCD Type 1) or **adding columns** (SCD Type 3), Type 2 **inserts new rows**:

```sql
-- Initial state (2023-01-01)
┌────┬──────────┬───────┬─────────┬─────────────────┬───────────────┬────────────┐
│ SK │ NK       │ Name  │ State   │ effective_start │ effective_end │ is_current │
├────┼──────────┼───────┼─────────┼─────────────────┼───────────────┼────────────┤
│ 1  │ CUST001  │ John  │ CA      │ 2023-01-01      │ 9999-12-31    │ true       │
└────┴──────────┴───────┴─────────┴─────────────────┴───────────────┴────────────┘

-- After John moves to Texas (2024-06-15)
┌────┬──────────┬───────┬─────────┬─────────────────┬───────────────┬────────────┐
│ SK │ NK       │ Name  │ State   │ effective_start │ effective_end │ is_current │
├────┼──────────┼───────┼─────────┼─────────────────┼───────────────┼────────────┤
│ 1  │ CUST001  │ John  │ CA      │ 2023-01-01      │ 2024-06-14    │ false      │ ← Old version
│ 2  │ CUST001  │ John  │ TX      │ 2024-06-15      │ 9999-12-31    │ true       │ ← Current version
└────┴──────────┴───────┴─────────┴─────────────────┴───────────────┴────────────┘

-- After another move to Florida (2025-01-10)
┌────┬──────────┬───────┬─────────┬─────────────────┬───────────────┬────────────┐
│ SK │ NK       │ Name  │ State   │ effective_start │ effective_end │ is_current │
├────┼──────────┼───────┼─────────┼─────────────────┼───────────────┼────────────┤
│ 1  │ CUST001  │ John  │ CA      │ 2023-01-01      │ 2024-06-14    │ false      │
│ 2  │ CUST001  │ John  │ TX      │ 2024-06-15      │ 2025-01-09    │ false      │
│ 3  │ CUST001  │ John  │ FL      │ 2025-01-10      │ 9999-12-31    │ true       │ ← Latest
└────┴──────────┴───────┴─────────┴─────────────────┴───────────────┴────────────┘
```

### Key Columns

| Column | Purpose |
|--------|---------|
| **SK (Surrogate Key)** | Auto-generated unique identifier for each version |
| **NK (Natural Key)** | Business key (e.g., `customer_id`) - same for all versions |
| **effective_start** | When this version became active |
| **effective_end** | When this version was superseded (9999-12-31 = current) |
| **is_current** | Boolean flag for latest version (for easy filtering) |
| **row_hash** | MD5/SHA hash of all attributes (detects actual changes) |

### Query Patterns

**Get current state (latest version only):**
```sql
SELECT * 
FROM dim_customer_scd
WHERE is_current = true;
```

**Point-in-time query (state as of specific date):**
```sql
SELECT * 
FROM dim_customer_scd
WHERE '2024-03-15' BETWEEN effective_start AND effective_end;
```

**Time-aware fact joins (accurate historical analysis):**
```sql
SELECT 
    c.customer_name,
    c.state,
    SUM(f.amount) as total_sales
FROM fact_sales f
JOIN dim_customer_scd c 
    ON f.customer_sk = c.sk  -- Join by surrogate key, not natural key!
WHERE f.order_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY c.customer_name, c.state;
```

### SCD Type Comparison

| Feature | Type 1 | Type 2 | Type 3 |
|---------|--------|--------|--------|
| **History** | ❌ No | ✅ Full | ⚠️ Limited |
| **Storage** | Minimal | Higher | Moderate |
| **Complexity** | Simple | Medium | Simple |
| **Use Case** | Corrections | Audit/Compliance | Before/After |
| **Example** | Fix typo | Track moves | "Previous Address" column |

### Implementation in This Project

- **Silver Layer**: `silver.dim_customer_scd` contains the full SCD Type 2 implementation with MERGE logic
- **Gold Layer**: 
  - `gold.dim_cliente_scd` - Replica for time-aware fact joins
  - `gold.dim_cliente` - Current snapshot (1 row per customer) for simpler queries
- **Fact Tables**: `fact_vendas` uses surrogate keys to join historically accurate customer states

---

## Repository Structure

```
workshop-databricks-data-modeling/
│
├── notebooks/                          # Interactive Databricks notebooks
│   ├── bronze_layer.ipynb             # Raw data generation (Faker)
│   ├── silver_layer.ipynb             # Data cleansing & SCD Type 2
│   └── gold_layer.ipynb               # Dimensional model creation
│
├── models/                            # SQL-focused query notebooks
│   ├── silver/
│   │   └── *.dbquery.ipynb            # Silver layer SQL queries
│   └── gold/
│       └── *.dbquery.ipynb            # Gold layer SQL queries
│
├── jobs/
│   └── data_warehouse_job.yaml        # Databricks Asset Bundle (DAB) job definition
│
└── README.md                          # This file
```

---

## Data Model

### Bronze Layer (Raw Data)

```
bronze.customers          bronze.products
├── customer_id (PK)     ├── product_id (PK)
├── first_name           ├── name
├── last_name            ├── category
├── email                ├── price
├── state                └── created_at
├── created_at           
└── updated_at           

bronze.orders            bronze.order_items
├── order_id (PK)        ├── order_item_id (PK)
├── customer_id (FK)     ├── order_id (FK)
├── order_date           ├── product_id (FK)
├── status               ├── quantity
├── total_amount         ├── unit_price
└── created_at           └── total_price
```

### Silver Layer (Cleansed Data)

```
silver.orders_clean              silver.dim_customer_scd (SCD Type 2)
├── order_id (PK)                ├── sk_customer (PK - Surrogate Key)
├── customer_id (FK)             ├── nk_customer_id (Natural Key)
├── order_date                   ├── first_name
├── status (normalized)          ├── last_name
├── total_amount (decimal)       ├── email
├── row_hash (md5)               ├── state
├── processed_at                 ├── effective_start
└── ... other fields             ├── effective_end
                                 ├── is_current (boolean)
silver.products_clean            ├── row_hash (md5)
├── product_id (PK)              └── created_at
├── name                         
├── category (normalized)        silver.order_items_clean
├── price (decimal)              ├── order_item_id (PK)
├── row_hash (md5)               ├── order_id (FK)
└── processed_at                 ├── product_id (FK)
                                 ├── quantity (int)
                                 ├── unit_price (decimal)
                                 ├── total_price (decimal)
                                 ├── row_hash (md5)
                                 └── processed_at
```

### Gold Layer (Star Schema)

```
DIMENSIONS:

dim_tempo                        dim_produto                    dim_cliente
├── tempo_sk (PK)               ├── sk_produto (PK)            ├── sk_cliente (PK)
├── data                        ├── nk_product_id              ├── nk_customer_id
├── ano                         ├── nome_produto               ├── nome_completo
├── mes                         ├── categoria                  ├── email
├── dia                         ├── preco                      ├── state
├── trimestre                   └── data_criacao               └── data_primeira_compra
└── dia_semana                  

dim_cliente_scd (Historical)
├── sk_cliente (PK)
├── nk_customer_id
├── first_name / last_name
├── email / state
├── effective_start
├── effective_end
└── is_current

FACTS:

fact_vendas (Time-Aware)           fact_vendas_current (Snapshot)
├── sk_venda (PK)                  ├── sk_venda (PK)
├── fk_tempo (FK → dim_tempo)      ├── fk_tempo (FK)
├── fk_produto (FK → dim_produto)  ├── fk_produto (FK)
├── fk_cliente (FK → dim_cliente_scd)  ├── fk_cliente (FK → dim_cliente)
├── order_id                       ├── quantidade
├── quantidade                     ├── preco_unitario
├── preco_unitario                 └── receita_liquida
├── desconto                       
└── receita_liquida                

VIEWS:

vw_vendas_diarias                  vw_receita_por_estado
├── Aggregated daily sales         ├── Revenue breakdown by customer state
└── KPIs: revenue, orders, items   └── Supports geographic analysis
```

---

## Getting Started

### Prerequisites

- **Databricks Workspace** (Community Edition, Trial, or Enterprise)
- **Cluster** with Delta Lake support (Runtime 13.3 LTS or higher recommended)
- **Python libraries**: Faker (auto-installed by notebooks)
- **Optional**: Unity Catalog (instructions provided for both scenarios)

### Step 1: Clone or Import Repository

**Option A: Using Databricks Repos (Recommended)**
```bash
# In Databricks:
# Repos → Add Repo → Paste Git URL
https://github.com/farias-thiago/workshop-databricks-data-modeling.git
```

**Option B: Manual Upload**
- Download notebooks from `notebooks/` folder
- Upload to your Databricks workspace via UI

### Step 2: Configure Catalog Settings

The notebooks default to Unity Catalog with catalog name: `workshop_modelagem`

**If you HAVE Unity Catalog:**
```python
# In notebooks, keep as-is:
CATALOG = "workshop_modelagem"  # or change to your catalog name
```

**If you DON'T HAVE Unity Catalog:**
```python
# In notebooks, set:
CATALOG = None  # This creates databases without catalog prefix
```

Then remove/comment out these lines in SQL cells:
```sql
-- USE CATALOG workshop_modelagem;  -- Comment this out
```

### Step 3: Create and Start Cluster

```
Cluster Configuration:
├── Runtime: 13.3 LTS or higher
├── Mode: Single Node (for free edition) or Standard
├── Node Type: Any available (e.g., i3.xlarge)
└── Python: 3.9+
```

### Step 4: Execute Notebooks in Order

**⚠️ IMPORTANT: Run in this exact sequence:**

1. **`notebooks/bronze_layer.ipynb`**
   - Installs Faker library
   - Creates Bronze schemas and tables
   - Generates synthetic data (customers, products, orders, order_items)
   - Runtime: ~2-3 minutes

2. **`notebooks/silver_layer.ipynb`**
   - Creates Silver schemas
   - Implements incremental MERGE logic
   - Sets up SCD Type 2 for customers
   - Applies data quality rules
   - Runtime: ~3-5 minutes

3. **`notebooks/gold_layer.ipynb`**
   - Creates Gold dimensional model
   - Builds dimensions and facts
   - Creates analytical views
   - Runtime: ~3-5 minutes

### Step 5: Validate the Results

Run these queries in a Databricks SQL notebook or SQL Warehouse:

```sql
-- Check row counts across layers
SELECT 'bronze.customers' as table_name, COUNT(*) as rows FROM bronze.customers
UNION ALL
SELECT 'silver.dim_customer_scd', COUNT(*) FROM silver.dim_customer_scd
UNION ALL
SELECT 'gold.dim_cliente', COUNT(*) FROM gold.dim_cliente
UNION ALL
SELECT 'gold.fact_vendas', COUNT(*) FROM gold.fact_vendas;

-- Preview daily sales
SELECT * FROM gold.vw_vendas_diarias ORDER BY data DESC LIMIT 10;

-- Check SCD Type 2 history (show customers with multiple versions)
SELECT 
    nk_customer_id,
    first_name,
    state,
    effective_start,
    effective_end,
    is_current
FROM silver.dim_customer_scd
ORDER BY nk_customer_id, effective_start;
```

---

## Incremental Processing & Idempotency

A key feature of production data pipelines is the ability to **rerun safely** without creating duplicates or inconsistent states.

### Core Principles

**1. Idempotency** = Same input + same code → same output (no matter how many times you run it)

**2. Incremental Processing** = Only process new/changed data (efficiency)

### Implementation Strategies

#### Row-Level Hashing
```python
# Generate deterministic hash of all columns
from pyspark.sql.functions import md5, concat_ws, col

df_with_hash = df.withColumn(
    "row_hash",
    md5(concat_ws("||", *[col(c) for c in df.columns]))
)
```

**Purpose**: Detect actual changes (not just timestamps)

#### Deduplication with Window Functions
```sql
-- Deterministic deduplication (keeps latest by timestamp)
WITH dedupe AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY created_at DESC, order_id
        ) as rn
    FROM bronze.orders
)
SELECT * FROM dedupe WHERE rn = 1;
```

**Purpose**: Ensure one record per business key

#### MERGE Operations
```sql
-- Idempotent MERGE (only updates if hash differs)
MERGE INTO silver.orders_clean AS target
USING source_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED AND target.row_hash != source.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Purpose**: Update only actual changes (skip no-op updates)

#### Watermark-Based Windows
```python
# Only look back N days for incremental processing
watermark_days = 90

df_incremental = spark.sql(f"""
    SELECT * 
    FROM bronze.orders
    WHERE created_at >= CURRENT_DATE - INTERVAL {watermark_days} DAYS
""")
```

**Purpose**: Limit processing window for performance

### SCD Type 2 Idempotency

The SCD Type 2 MERGE logic ensures:
- ✅ Rerunning with same data = no duplicates
- ✅ Effective dates only change if attributes actually changed (via row_hash)
- ✅ `is_current` flag correctly maintained
- ✅ No orphaned records or gaps in history

---

## Job Orchestration

### Databricks Asset Bundle (DAB)

The `jobs/data_warehouse_job.yaml` defines a complete orchestration workflow with task dependencies.

**Job Structure:**
```
┌─────────────────────────────────────────────────────┐
│           E-Commerce Data Warehouse Job             │
├─────────────────────────────────────────────────────┤
│                                                     │
│  SILVER LAYER (Parallel)                            │
│  ├── orders_clean                                   │
│  ├── order_items_clean                              │
│  ├── products_clean                                 │
│  └── dim_customer_scd                               │
│           │                                         │
│           ▼                                         │
│  GOLD LAYER (Sequential Dependencies)               │
│  ├── dim_tempo                                      │
│  ├── dim_produto                                    │
│  ├── dim_cliente_scd                                │
│  │     │                                            │
│  │     ▼                                            │
│  └── dim_cliente ─┬─▶ fact_vendas                  │
│                    └─▶ fact_vendas_current         │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Setup Requirements

**For SQL Warehouse execution:**
1. Create queries in Databricks SQL Editor (copy from notebooks)
2. Note the `query_id` for each query
3. Update `jobs/data_warehouse_job.yaml` with your IDs:
   ```yaml
   query_id: "your-actual-query-id-here"
   warehouse_id: "your-warehouse-id-here"
   ```

**For notebook execution (alternative):**
Run notebooks sequentially via Jobs UI or CLI:
```bash
# Using Databricks CLI
databricks jobs create --json-file jobs/notebook_job.json
```

### Scheduling

Set schedule in the YAML or via UI:
```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
  timezone_id: "America/Sao_Paulo"
```

---

## Query Examples

### Business Intelligence Queries

**1. Daily Sales Trend**
```sql
SELECT 
    data,
    receita_liquida,
    total_pedidos,
    total_itens,
    receita_liquida / total_pedidos AS ticket_medio
FROM gold.vw_vendas_diarias
WHERE data >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY data DESC;
```

**2. Top Selling Products**
```sql
SELECT 
    p.nome_produto,
    p.categoria,
    SUM(f.quantidade) AS unidades_vendidas,
    SUM(f.receita_liquida) AS receita_total
FROM gold.fact_vendas f
JOIN gold.dim_produto p ON f.fk_produto = p.sk_produto
GROUP BY p.nome_produto, p.categoria
ORDER BY receita_total DESC
LIMIT 10;
```

**3. Customer Lifetime Value (Current State)**
```sql
SELECT 
    c.nome_completo,
    c.state,
    COUNT(DISTINCT f.order_id) AS total_pedidos,
    SUM(f.receita_liquida) AS ltv,
    AVG(f.receita_liquida) AS ticket_medio
FROM gold.fact_vendas_current f
JOIN gold.dim_cliente c ON f.fk_cliente = c.sk_cliente
GROUP BY c.nome_completo, c.state
ORDER BY ltv DESC
LIMIT 20;
```

**4. Revenue by Customer State (Time-Aware - Respects SCD Type 2)**
```sql
SELECT 
    c.state AS estado_no_momento_da_compra,
    t.ano,
    t.mes,
    SUM(f.receita_liquida) AS receita_total
FROM gold.fact_vendas f
JOIN gold.dim_cliente_scd c ON f.fk_cliente = c.sk_cliente
JOIN gold.dim_tempo t ON f.fk_tempo = t.tempo_sk
GROUP BY c.state, t.ano, t.mes
ORDER BY t.ano, t.mes, receita_total DESC;
```

**5. Customer Migration Analysis (SCD Type 2 Power)**
```sql
-- Find customers who moved between states
WITH customer_moves AS (
    SELECT 
        nk_customer_id,
        first_name,
        last_name,
        state,
        effective_start,
        effective_end,
        LAG(state) OVER (PARTITION BY nk_customer_id ORDER BY effective_start) AS previous_state
    FROM silver.dim_customer_scd
)
SELECT 
    nk_customer_id,
    first_name || ' ' || last_name AS nome,
    previous_state AS estado_anterior,
    state AS estado_novo,
    effective_start AS data_mudanca
FROM customer_moves
WHERE previous_state IS NOT NULL 
  AND previous_state != state
ORDER BY effective_start DESC;
```

**6. Month-over-Month Growth**
```sql
WITH monthly_sales AS (
    SELECT 
        ano,
        mes,
        SUM(receita_liquida) AS receita
    FROM gold.vw_vendas_diarias
    GROUP BY ano, mes
)
SELECT 
    ano,
    mes,
    receita,
    LAG(receita) OVER (ORDER BY ano, mes) AS receita_mes_anterior,
    ROUND(
        (receita - LAG(receita) OVER (ORDER BY ano, mes)) / 
        LAG(receita) OVER (ORDER BY ano, mes) * 100, 
        2
    ) AS crescimento_percentual
FROM monthly_sales
ORDER BY ano, mes;
```

---

## Best Practices

### 1. **Always Use Delta Lake**
- ACID transactions
- Time travel (`VERSION AS OF`, `TIMESTAMP AS OF`)
- Schema evolution
- Efficient upserts with MERGE

### 2. **Implement Data Quality Checks**
```python
# Example: Validate no nulls in critical columns
quality_check = spark.sql("""
    SELECT COUNT(*) as null_count
    FROM silver.orders_clean
    WHERE order_id IS NULL OR customer_id IS NULL
""")

assert quality_check.first()["null_count"] == 0, "Data quality check failed!"
```

### 3. **Use Surrogate Keys for Dimensions**
- Never join facts to dimensions using natural keys directly
- SCD Type 2 requires surrogate keys to maintain history
- Generate using `ROW_NUMBER()` or `MONOTONICALLY_INCREASING_ID()`

### 4. **Partition Large Tables**
```sql
CREATE TABLE gold.fact_vendas_partitioned
PARTITIONED BY (ano INT, mes INT)
AS SELECT * FROM gold.fact_vendas;
```

### 5. **Monitor Pipeline Performance**
```sql
-- Track processing metrics
CREATE TABLE IF NOT EXISTS ops.pipeline_metrics (
    pipeline_name STRING,
    layer STRING,
    rows_processed BIGINT,
    execution_time_seconds INT,
    run_timestamp TIMESTAMP
);
```

### 6. **Document Data Lineage**
Use tools like:
- Unity Catalog lineage tracking
- `DESCRIBE EXTENDED` for table metadata
- Comments on tables/columns

```sql
COMMENT ON TABLE gold.fact_vendas IS 
'Sales fact table at order item grain. Time-aware joins to dim_cliente_scd.';
```

---

## Troubleshooting

### Common Issues & Solutions

#### ❌ **Error: `USE CATALOG` command not supported**

**Cause**: Your workspace doesn't have Unity Catalog enabled.

**Solution**:
```python
# In all notebooks, set:
CATALOG = None

# Remove/comment in SQL cells:
# USE CATALOG workshop_modelagem;
```

#### ❌ **Error: Database/Schema already exists**

**Cause**: Running notebooks multiple times.

**Solution**: Bronze uses `CREATE OR REPLACE`, but if needed:
```sql
DROP DATABASE IF EXISTS bronze CASCADE;
DROP DATABASE IF EXISTS silver CASCADE;
DROP DATABASE IF EXISTS gold CASCADE;
```

#### ❌ **Faker module not found**

**Cause**: Library not installed on cluster.

**Solution**: First cell of `bronze_layer.ipynb` handles this:
```python
%pip install Faker
dbutils.library.restartPython()  # Required!
```

#### ❌ **MERGE operation is slow**

**Causes**: 
- Large watermark window
- No partitioning
- Missing Z-ORDER optimization

**Solutions**:
```sql
-- 1. Reduce watermark window
WHERE created_at >= CURRENT_DATE - INTERVAL 30 DAYS  -- Instead of 90

-- 2. Optimize table layout
OPTIMIZE silver.orders_clean
ZORDER BY (order_date, customer_id);

-- 3. Partition large tables
ALTER TABLE silver.orders_clean 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

#### ❌ **SCD Type 2 creating duplicate current records**

**Cause**: Missing `is_current = false` update in MERGE logic.

**Solution**: Check the MERGE has this WHEN MATCHED clause:
```sql
WHEN MATCHED AND source.row_hash != target.row_hash AND target.is_current = true
THEN UPDATE SET 
    effective_end = CURRENT_DATE - INTERVAL 1 DAY,
    is_current = false
```

#### ❌ **Job fails with "Query not found" error**

**Cause**: `query_id` in YAML doesn't match your workspace.

**Solution**: 
1. Create queries in SQL Editor
2. Copy query IDs from URL: `https://...sqlanalytics/...queries/{query_id}`
3. Update YAML with your IDs

#### ⚠️ **Performance Degradation Over Time**

**Causes**:
- Small files proliferation
- Lack of table optimization

**Solutions**:
```sql
-- Regular maintenance
OPTIMIZE bronze.orders;
OPTIMIZE silver.orders_clean;
OPTIMIZE gold.fact_vendas ZORDER BY (fk_tempo, fk_cliente);

-- Vacuum old versions (after 7 days)
VACUUM bronze.orders RETAIN 168 HOURS;
```

---

## Next Steps

### Extend This Workshop

**1. Add More SCD Types**
- Implement SCD Type 1 for products (overwrite, no history)
- Implement SCD Type 3 for "previous vs. current" comparisons

**2. Real-Time Streaming**
```python
# Replace Bronze batch loads with Structured Streaming
df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("/path/to/landing/")
    
df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .table("bronze.orders")
```

**3. Data Quality Framework**
- Integrate Great Expectations
- Add automated data profiling
- Set up anomaly detection

**4. Advanced Analytics**
- Build aggregated rollup tables (daily → weekly → monthly)
- Implement bridge tables for many-to-many relationships
- Create conformed dimensions for multi-fact star schemas

**5. CI/CD Pipeline**
```yaml
# Example: GitHub Actions + Databricks CLI
name: Deploy DW
on: [push]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to Databricks
        run: |
          databricks bundle deploy --target prod
```

**6. Monitoring & Observability**
- Set up query profiling
- Track data freshness SLAs
- Monitor cost per pipeline

**7. Semantic Layer**
- Connect to dbt for metric definitions
- Build a dbt project on top of Gold layer
- Implement data contracts

---

## Additional Resources

### Databricks Documentation
- [Delta Lake Guide](https://docs.databricks.com/delta/)
- [Unity Catalog](https://docs.databricks.com/unity-catalog/)
- [Databricks SQL](https://docs.databricks.com/sql/)
- [Jobs & Workflows](https://docs.databricks.com/workflows/)

### Dimensional Modeling
- 📘 *The Data Warehouse Toolkit* by Ralph Kimball (SCD Types, Star Schema)
- 📘 *Building the Data Warehouse* by Bill Inmon
- 🎓 [Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)

### Delta Lake & Lakehouse
- [Delta Lake Documentation](https://delta.io/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Lakehouse Paradigm](https://www.databricks.com/product/data-lakehouse)

---

## Contributing

Contributions are welcome! Areas for improvement:
- Additional SCD types (Type 1, Type 3, Type 6)
- Streaming ingestion examples
- dbt integration
- More complex star schemas
- Performance tuning examples

---

## License

This project is for **educational and workshop purposes**. Feel free to adapt for your organization's needs.

**Attribution**: Please maintain attribution if sharing or forking this repository.

---

## Acknowledgments

Built to demonstrate real-world data engineering patterns on Databricks, combining:
- Industry best practices (Kimball dimensional modeling)
- Modern lakehouse architecture (Medallion pattern)
- Production-ready techniques (idempotency, SCD Type 2, incremental processing)

Perfect for learning, teaching, or as a starter template for production data warehouses.

---

**Questions or Issues?** Open an issue on GitHub or reach out to the community!

**Happy Data Modeling! 🚀📊**

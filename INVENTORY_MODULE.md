# Inventory Intelligence Module — SAP Ingestion & Analytics

## Overview

This module extends the Inventory Intelligence module with direct SAP file
ingestion (MB51, ME2M, MC.9) into MySQL, computed analytics tables, and a
full REST API for querying inventory, procurement, and vendor performance data.

## Fiscal Year Convention

The organisation uses an **April–March fiscal year**:

| Fiscal Year | Calendar Range           | Period 1 | Period 12 |
|-------------|--------------------------|----------|-----------|
| FY2024      | April 2023 – March 2024  | April    | March     |
| FY2025      | April 2024 – March 2025  | April    | March     |

All period calculations, fiscal_year columns, and fiscal_period columns follow
this convention.

---

## Running the Migration

The schema is defined in raw SQL (not Alembic) to give full control over
indexes and MySQL-specific types.

```bash
mysql -u <user> -p <database> < migrations/sql/001_inventory_schema.sql
```

This creates six tables: `mb51_movements`, `me2m_purchase_orders`,
`mc9_stock_analysis`, `stock_register_monthly`, `vendor_scorecard`, and
`data_load_log`, plus composite indexes.

All `CREATE TABLE` statements use `IF NOT EXISTS` so the script is safe to
re-run.

---

## Uploading SAP Files

### Via the API

```
POST /inventory/upload
Content-Type: multipart/form-data
```

| Field        | Required | Description                                  |
|--------------|----------|----------------------------------------------|
| source       | Yes      | `MB51`, `ME2M`, or `MC9`                     |
| file         | Yes      | `.xlsx` or `.csv` SAP export (max 50 MB)     |
| fiscal_year  | No       | Override fiscal year (auto-detected if absent)|

**Response:**
```json
{
  "status": "success",
  "rows_loaded": 12345,
  "rows_rejected": 0,
  "warnings": []
}
```

After a successful upload the module automatically recomputes:
- `stock_register_monthly` (after MC9 or MB51 uploads)
- `vendor_scorecard` (after ME2M uploads)

### Header Tolerance

SAP exports use varying column headers. Each loader defines an alias
dictionary that maps common SAP label variants to canonical names. For
example, "Posting Date", "Pstng Date", "posting date" all map to
`posting_date`.

---

## Triggering a Recompute

```
POST /inventory/recompute
Content-Type: application/json
```

```json
{
  "fiscal_year": 2025,
  "scope": "all"
}
```

| scope             | Rebuilds                        |
|-------------------|---------------------------------|
| stock_register    | stock_register_monthly only     |
| vendor_scorecard  | vendor_scorecard only           |
| all               | Both tables                     |

---

## API Routes

All routes are prefixed with `/inventory` and require authentication +
inventory module access.

### Data Management

| Method | Path                        | Description                        |
|--------|-----------------------------|------------------------------------|
| POST   | /inventory/upload           | Upload SAP file                    |
| POST   | /inventory/recompute        | Rebuild computed tables            |
| GET    | /inventory/load-history     | Last 50 load log entries           |

### Stock Register

| Method | Path                        | Params                              |
|--------|-----------------------------|------------------------------------|
| GET    | /inventory/stock-register   | fiscal_year (req), plant, material_no, format (json\|excel) |

### Analytics

| Method | Path                                    | Params                                        |
|--------|-----------------------------------------|-----------------------------------------------|
| GET    | /inventory/analytics/yoy-spend          | fiscal_years (comma-sep, req), plant, material_group |
| GET    | /inventory/analytics/abc                | fiscal_year (req), plant                       |
| GET    | /inventory/analytics/vendor-scorecard   | fiscal_year (req), grade (A\|B\|C\|D)         |
| GET    | /inventory/analytics/dead-stock         | plant, months (default 6)                      |
| GET    | /inventory/analytics/open-po-aging      | plant                                          |
| GET    | /inventory/analytics/price-evolution    | material_no (req), plant (req)                 |
| GET    | /inventory/analytics/consumption-trend  | material_no (req), plant (req), months (default 24) |
| GET    | /inventory/analytics/forecast           | material_no (req), plant (req), method, periods_ahead |
| GET    | /inventory/analytics/xyz                | fiscal_year (req), plant                       |

### Forecast Methods

The `method` parameter for `/analytics/forecast` accepts:
- `simple_moving_avg` — 12-month simple average
- `weighted_moving_avg` — linearly weighted 12-month average (default)
- `exponential_smoothing` — SES with alpha = 0.3, 24-month lookback
- `seasonal_index` — seasonal decomposition (requires 3+ years of data)

---

## File Structure

```
app/modules/inventory/
├── __init__.py                    # Blueprint definition
├── routes.py                      # All routes (existing + new SAP API)
├── ingestion/
│   ├── __init__.py
│   ├── base_loader.py             # Abstract base: file I/O, header norm, upsert
│   ├── mb51_loader.py             # MB51 movement document loader
│   ├── me2m_loader.py             # ME2M purchase order loader
│   └── mc9_loader.py              # MC.9 stock analysis loader
└── analytics/
    ├── __init__.py
    ├── stock_register.py           # Build stock_register_monthly
    ├── vendor_grading.py           # Build vendor_scorecard
    ├── queries.py                  # Phase-1 analytics queries
    └── forecast.py                 # Forecasting + XYZ classification
```

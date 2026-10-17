# AI/BI Dashboard — "UrbanMart Sales Performance"

All datasets read **gold tables only**. The SQL of each dataset is in
[`notebooks/07_analytics.sql`](../notebooks/07_analytics.sql) (cell title in brackets).

## Datasets

| Dataset | Source cell in 07 | Used by |
|---|---|---|
| `kpis` | *KPIs 2024-2025* | Counters row |
| `monthly_store` | *Q1 — monthly revenue / margin / units by region and store* | Revenue trend, region bar |
| `category_brand` | *Q2 — margin by category and brand* | Margin by category & brand |
| `top_customers` | *Q3 — top 5 customers per current segment* | Top customers table |
| `returns` | *Q4 — return rate by category and store type* | Return-rate heatmap |
| `daytype_channel` | *Q5 — day type x channel* | Weekday vs weekend |

## Layout (one page, 7 visuals)

| # | Visual | Type | Fields | Answers |
|---|---|---|---|---|
| 1 | Net revenue · Gross margin · Margin % · Orders · AOV · Return rate | 6 **Counters** | `kpis` | Overview |
| 2 | Monthly net revenue by region | **Line** | x = `month_start_date` (monthly), y = SUM(`net_revenue`), color = `region` | Q1 |
| 3 | Revenue & margin by store | **Bar** (horizontal) | y = `store_name`, x = SUM(`net_revenue`), SUM(`gross_margin`) | Q1 |
| 4 | Margin by category and brand | **Bar** (stacked) | x = `category`, y = SUM(`gross_margin`), color = `brand` | Q2 |
| 5 | Top 5 customers per segment | **Table** | `segment`, `segment_rank`, `full_name`, `city`, `net_revenue`, `orders` | Q3 |
| 6 | Return rate by category × store type | **Heatmap** (or pivot) | x = `store_type`, y = `category`, color = `line_return_rate_pct` | Q4 |
| 7 | Avg revenue per day: weekday vs weekend, online vs physical | **Bar** (grouped) | x = `day_type`, y = `avg_revenue_per_day`, color = `channel` | Q5 |

**Filters (global):** `year_month` range, `region`, `channel`.

## How to build it (≈ 15 min)

1. **Dashboards → Create dashboard**, name it *UrbanMart Sales Performance*.
2. **Data** tab → **Create from SQL** → paste one query per dataset from notebook 07 → rename the dataset (names above).
   Replace the `USE …` context by fully qualified names, e.g. `urbanmart.gold.fact_sales`, or pick the catalog / schema in the dataset editor.
3. **Canvas** tab → add a visualization per row of the layout table and choose the dataset and fields.
4. Add the filters, then **Publish**. Take a screenshot and save it as `docs/img/dashboard.png` for the repository README.

## Key findings (2024-01-01 → 2025-12-30, Completed orders)

- **€9.14 M net revenue, €2.69 M gross margin (29.4 %)**, 7,241 orders, AOV **€1,262**, order return rate **5.9 %**.
- **Attica** (5 stores incl. online) brings 39 % of revenue (€3.59 M); margin % is flat across regions (28.9 %–29.8 %).
- **Grocery** is the most profitable category (€649 k, 31.3 %). Top brand × category: **Nova Electronics** (€229 k margin);
  **Olympus Electronics** has the weakest margin rate (14.5 %).
- Returns are highest for **Electronics in Malls** (7.2 % of lines) and **Home on High Streets** (7.1 %); lowest for **online Grocery** (4.6 %).
- Weekends sell **as much per day** as weekdays (physical €11.5 k/day both; online ≈ €1.0 k/day both) — the weekday total is larger only because there are more weekdays.
- Online is 8 % of revenue with a similar margin rate to physical stores.

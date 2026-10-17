# Data-Quality Log — UrbanMart extract

Profiling was done on the **bronze** tables (all columns STRING, untouched) in
[`notebooks/02_bronze_profiling.sql`](../notebooks/02_bronze_profiling.sql).
The same log is materialised as the table **`bronze.dq_issue_log`**, with the counts computed from the data at run time.

## Issues and handling rules

| ID | Table · column | Issue | Rows | Severity | Handling rule | Reject code |
|---|---|---|---:|---|---|---|
| DQ01 | customers · `customer_id` | Exact duplicate customer records (8 IDs appear twice, identical attributes) | 8 | High | Keep one row per `customer_id` (`ROW_NUMBER`); extra copies go to `etl_rejects` | `DUPLICATE_CUSTOMER` |
| DQ02 | customers · `city` | City in UPPER CASE with a trailing space (e.g. `"IOANNINA "`) — would create 14 cities instead of 7 | 26 | Medium | `initcap(trim(city))` | — |
| DQ03 | customers · `email` | Missing e-mail | 15 | Low | Keep the customer, `email = NULL` (not used in analytics) | — |
| DQ04 | products · category, subcategory, brand, cost, price | Placeholder `P9999 "Unknown Item"`: blank category / subcategory / brand, cost = price = 0 | 1 | Medium | Exclude from `dim_product`; the gold unknown member `-1` plays this role. Not referenced by any order line | `INVALID_PRODUCT` |
| DQ05 | stores · `size_sqm` | Online store `S900` has `size_sqm = 0` | 1 | Low | `NULL` (not applicable to online) so that size-based averages are not distorted | — |
| DQ06 | orders · `customer_id` | Blank customer (guest checkout) | 312 | Medium | Keep the order; `customer_key = -1` (*Guest / Unknown*) | — |
| DQ07 | orders · `order_ts` | 76 timestamps in `dd/MM/yyyy HH:mm` instead of `yyyy-MM-dd HH:mm:ss`; time is always `12:00` (probably defaulted) | 76 | High | `coalesce(try_to_timestamp(ISO), try_to_timestamp(dd/MM/yyyy HH:mm))`; flagged with `is_ts_reformatted`; still-unparseable values would be rejected | `INVALID_ORDER_TS` |
| DQ08 | order_items · `quantity` | Negative quantity (−1…−4). 105 of 113 sit on *Completed* orders, so they are **not** return lines | 113 | High | Reject the line; `CHECK (quantity > 0)` on the fact | `NON_POSITIVE_QUANTITY` |
| DQ09 | order_items · `order_id` | Line `L9999999` points to order `O999999`, which does not exist | 1 | High | Reject the line — no date, store or customer can be derived | `ORPHAN_ORDER` |
| DQ10 | order_items · `product_id` | Line `L9999998` sells product `PX000`, missing from the catalogue | 1 | Medium | Keep the sale (the revenue is real), `product_key = -1`; cost and margin `NULL` because cost is unknown | — |
| DQ11 | order_items | 32 lines have the same order, product, qty, price and discount as another line, but a different `order_line_id` | 32 | Info | **Investigated, not a defect**: with ~3 random lines per order this rate is expected by chance and the business key is unique → kept | — |
| DQ12 | orders · `order_status` | 453 *Returned* and 302 *Cancelled* orders | 755 | Info | Business rule, not a defect: loaded to the fact; status lives in `dim_order_profile`; revenue = *Completed* only | — |

## Checks that found nothing

Profiled and clean: duplicate keys in products / stores / orders / order_items / customer_changes; `birth_date`, `signup_date`,
`open_date`, `effective_date` formats; non-numeric prices; discounts outside 0–1; orders → stores and orders → customers
referential integrity; orders without lines; customer_changes → customers. These rules still run in silver, so a future extract
with such a problem would be caught.

## Observations (no rule applied)

- `gender` often does not match the first name (e.g. *Giorgos · F*). There is no reliable source to correct it, so it is loaded as is
  and should not be used for analysis.
- 21 products are inactive (`is_active = N`) but have historical sales. This is expected for discontinued products.
- 188 orders are dated **before** the customer's `signup_date`. Probably the customer bought before registering a loyalty account; the
  orders are kept, and the first SCD2 version therefore starts on 1900-01-01 instead of the signup date.
- 4 *Completed* orders lose all their lines to DQ08, so they have no fact rows (7,245 completed orders in the source → 7,241 in the fact).

## Outcome

| Entity | Bronze rows | Silver rows | Rejected |
|---|---:|---:|---:|
| customers | 508 | 500 | 8 |
| customer_changes | 25 | 25 | 0 |
| products | 76 | 75 | 1 |
| stores | 13 | 13 | 0 |
| orders | 8,000 | 8,000 | 0 |
| order_items | 24,111 | 23,997 | 114 |

**24,111 bronze lines − 114 rejected lines = 23,997 fact rows** (validation check V01).

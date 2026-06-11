-- ============================================================
-- GOLD LAYER — aggregate tables (materialized views)
-- Common retail metrics computed from the star schema.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- agg_monthly_sales — revenue & order KPIs
-- grain: order_year × order_month × product_category × state
-- ─────────────────────────────────────────────────────────────
CREATE OR REFRESH MATERIALIZED VIEW cowork_fable5.gold_fa5_v1.agg_monthly_sales
COMMENT 'Monthly sales KPIs by product category and state (gross/net revenue, discounts, order outcomes)'
CLUSTER BY (order_year, order_month)
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  order_year,
  order_month,
  product_category,
  state,
  COUNT(*)                                                   AS total_orders,
  SUM(quantity)                                              AS total_quantity,
  SUM(gross_amount)                                          AS gross_revenue,
  SUM(discount_amount)                                       AS total_discount,
  SUM(net_amount)                                            AS net_revenue,
  ROUND(AVG(net_amount), 2)                                  AS avg_order_value,
  ROUND(AVG(discount_pct), 2)                                AS avg_discount_pct,
  SUM(CASE WHEN order_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_orders,
  SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
  SUM(CASE WHEN order_status = 'Returned'  THEN 1 ELSE 0 END) AS returned_orders,
  ROUND(AVG(days_to_ship), 1)                                AS avg_days_to_ship
FROM cowork_fable5.gold_fa5_v1.fact_sales
GROUP BY order_year, order_month, product_category, state;

-- ─────────────────────────────────────────────────────────────
-- agg_customer_demographics — customer-behaviour KPIs
-- grain: state × gender × age_band (fact ⋈ dim_customer)
-- ─────────────────────────────────────────────────────────────
CREATE OR REFRESH MATERIALIZED VIEW cowork_fable5.gold_fa5_v1.agg_customer_demographics
COMMENT 'Customer demographic KPIs: spend, AOV, return and cancellation rates by state, gender, age band'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  f.state,
  COALESCE(c.gender, 'Unknown')        AS gender,
  CASE
    WHEN c.customer_age IS NULL THEN 'Unknown'
    WHEN c.customer_age < 25    THEN '18-24'
    WHEN c.customer_age < 35    THEN '25-34'
    WHEN c.customer_age < 50    THEN '35-49'
    WHEN c.customer_age < 65    THEN '50-64'
    ELSE '65+'
  END                                  AS age_band,
  COUNT(DISTINCT f.customer_id)        AS distinct_customers,
  COUNT(*)                             AS total_orders,
  SUM(f.net_amount)                    AS net_revenue,
  ROUND(AVG(f.net_amount), 2)          AS avg_order_value,
  ROUND(SUM(CASE WHEN f.order_status = 'Returned'  THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS return_rate_pct,
  ROUND(SUM(CASE WHEN f.order_status = 'Cancelled' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS cancellation_rate_pct
FROM cowork_fable5.gold_fa5_v1.fact_sales f
LEFT JOIN cowork_fable5.gold_fa5_v1.dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY f.state, COALESCE(c.gender, 'Unknown'),
  CASE
    WHEN c.customer_age IS NULL THEN 'Unknown'
    WHEN c.customer_age < 25    THEN '18-24'
    WHEN c.customer_age < 35    THEN '25-34'
    WHEN c.customer_age < 50    THEN '35-49'
    WHEN c.customer_age < 65    THEN '50-64'
    ELSE '65+'
  END;

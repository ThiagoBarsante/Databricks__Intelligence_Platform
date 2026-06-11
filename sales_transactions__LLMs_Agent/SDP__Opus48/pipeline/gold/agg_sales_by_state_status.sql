-- Gold aggregate #2: sales by geography (state) x fulfilment outcome (order_status).
-- Demonstrates a star-schema join: fact_sales -> dim_location (SCD Type 2, current rows).
CREATE OR REFRESH MATERIALIZED VIEW cowork_op48.gold.agg_sales_by_state_status
COMMENT 'Aggregate #2 - sales by state and order status (fact joined to SCD2 location dim)'
AS
SELECT
  l.state,
  f.order_status,
  COUNT(*)                    AS order_count,
  SUM(f.quantity)             AS total_units,
  SUM(f.net_amount)           AS total_net,
  ROUND(AVG(f.net_amount), 2) AS avg_order_value,
  COUNT(DISTINCT f.customer_id) AS distinct_customers
FROM cowork_op48.gold.fact_sales f
JOIN cowork_op48.gold.dim_location l
  ON f.location_id = l.location_id
 AND l.`__END_AT` IS NULL          -- current version of SCD2 dimension
GROUP BY l.state, f.order_status;

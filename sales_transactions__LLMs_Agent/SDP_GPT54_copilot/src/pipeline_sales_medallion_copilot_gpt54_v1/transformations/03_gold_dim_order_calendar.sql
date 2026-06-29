CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.${gold_schema}.dim_order_calendar
COMMENT 'Reusable calendar dimension for order and ship dates.'
AS
WITH all_dates AS (
  SELECT order_date AS calendar_date
  FROM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean
  UNION
  SELECT ship_date AS calendar_date
  FROM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean
  WHERE ship_date IS NOT NULL
)
SELECT
  calendar_date,
  YEAR(calendar_date) AS calendar_year,
  QUARTER(calendar_date) AS calendar_quarter,
  MONTH(calendar_date) AS calendar_month,
  DATE_FORMAT(calendar_date, 'MMMM') AS month_name,
  WEEKOFYEAR(calendar_date) AS week_of_year,
  DAYOFMONTH(calendar_date) AS day_of_month,
  DAYOFWEEK(calendar_date) AS day_of_week,
  DATE_FORMAT(calendar_date, 'EEEE') AS day_name,
  CASE
    WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS is_weekend
FROM all_dates;

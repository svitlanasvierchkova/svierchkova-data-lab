-- Run against the Gold Lakehouse SQL analytics endpoint; adjust names after schema mapping.
SELECT COUNT(*) AS sales_rows, SUM(amount_usd) AS sales_usd FROM fact_sales;
SELECT COUNT(*) AS payment_rows, SUM(amount_usd) AS payment_usd FROM fact_payments;
SELECT COUNT(*) AS missing_product FROM fact_sales f LEFT JOIN dim_product d ON f.product_key=d.product_key WHERE d.product_key IS NULL;
SELECT COUNT(*) AS missing_customer FROM fact_sales f LEFT JOIN dim_customer d ON f.customer_key=d.customer_key WHERE d.customer_key IS NULL;
-- Save counts and sums before and after a second full pipeline run; they must match.

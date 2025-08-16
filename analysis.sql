-- 1) Top 10 highest revenue-generating products
SELECT product_id, SUM(COALESCE(sales,0)) AS revenue
FROM df_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- 2) Top 5 products in each region (by revenue)
WITH agg AS (
  SELECT region, product_id, SUM(COALESCE(sales,0)) AS revenue
  FROM df_orders
  GROUP BY region, product_id
)
SELECT region, product_id, revenue
FROM (
  SELECT region, product_id, revenue,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
  FROM agg
) t
WHERE rn <= 5
ORDER BY region, revenue DESC;

-- 3) 2022 vs 2023 monthly sales comparison
WITH m AS (
  SELECT EXTRACT(YEAR FROM order_date)::int AS order_year,
         EXTRACT(MONTH FROM order_date)::int AS order_month,
         SUM(COALESCE(sales,0)) AS revenue
  FROM df_orders
  WHERE order_date IS NOT NULL
  GROUP BY 1,2
)
SELECT order_month,
       SUM(CASE WHEN order_year=2022 THEN revenue ELSE 0 END) AS sales_2022,
       SUM(CASE WHEN order_year=2023 THEN revenue ELSE 0 END) AS sales_2023
FROM m
GROUP BY order_month
ORDER BY order_month;

-- 4) For each category, the month with highest sales
WITH by_month AS (
  SELECT category,
         date_trunc('month', order_date)::date AS month_start,
         SUM(COALESCE(sales,0)) AS revenue
  FROM df_orders
  WHERE order_date IS NOT NULL
  GROUP BY 1,2
),
ranked AS (
  SELECT category, month_start, revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM by_month
)
SELECT category, month_start, revenue
FROM ranked
WHERE rn = 1
ORDER BY category;

-- 5) Sub-category with highest growth by profit (2023 vs 2022)
WITH yearly AS (
  SELECT sub_category,
         EXTRACT(YEAR FROM order_date)::int AS order_year,
         SUM(COALESCE(profit,0)) AS profit_total
  FROM df_orders
  WHERE order_date IS NOT NULL
  GROUP BY 1,2
),
pivoted AS (
  SELECT sub_category,
         SUM(CASE WHEN order_year = 2022 THEN profit_total ELSE 0 END) AS profit_2022,
         SUM(CASE WHEN order_year = 2023 THEN profit_total ELSE 0 END) AS profit_2023
  FROM yearly
  GROUP BY sub_category
)
SELECT sub_category, profit_2022, profit_2023,
       (profit_2023 - profit_2022) AS abs_growth,
       CASE WHEN profit_2022 <> 0
            THEN 100.0 * (profit_2023 - profit_2022)/ABS(profit_2022)
            ELSE NULL END AS pct_growth
FROM pivoted
ORDER BY abs_growth DESC
LIMIT 1;

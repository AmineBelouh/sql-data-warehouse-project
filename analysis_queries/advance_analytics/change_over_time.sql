-- ============================================================================
-- CHANGE-OVER-TIME ANALYSIS
-- Metrics: Total Sales (Revenue), Active Customers, Quantity Sold
-- Purpose: Analyze monthly business performance trends over time
-- ============================================================================


-- ---------------------------------------------------------------------------
-- APPROACH 1: EXTRACT
-- Use EXTRACT when you need numeric year/month values for grouping or filtering
-- ---------------------------------------------------------------------------
SELECT
	EXTRACT(YEAR FROM order_date) AS order_year,
	EXTRACT(MONTH FROM order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
	EXTRACT(YEAR FROM order_date),
	EXTRACT(MONTH FROM order_date)
ORDER BY
	order_year,
	order_month;


-- ---------------------------------------------------------------------------
-- APPROACH 2: DATE_PART
-- Equivalent to EXTRACT; preferred by some for readability and flexibility
-- ---------------------------------------------------------------------------
SELECT
	DATE_PART('year', order_date) AS order_year,
	DATE_PART('month', order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
	DATE_PART('year', order_date),
	DATE_PART('month', order_date)
ORDER BY
	DATE_PART('year', order_date),
	DATE_PART('month', order_date);


-- ---------------------------------------------------------------------------
-- APPROACH 3: DATE_TRUNC (RECOMMENDED)
-- Best practice for time-series analysis and BI tools
-- Produces a single, continuous date column (year-month)
-- ---------------------------------------------------------------------------
SELECT
	DATE_TRUNC('month', order_date)::DATE AS year_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
	DATE_TRUNC('month', order_date)
ORDER BY
	DATE_TRUNC('month', order_date);


-- ---------------------------------------------------------------------------
-- APPROACH 4: TO_CHAR
-- Converts dates to text; useful for reporting, NOT recommended for analytics
-- ---------------------------------------------------------------------------
SELECT
	TO_CHAR(order_date, 'YYYY-Month') AS year_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
	TO_CHAR(order_date, 'YYYY-MM'),
	TO_CHAR(order_date, 'YYYY-Month')
ORDER BY
	TO_CHAR(order_date, 'YYYY-MM');

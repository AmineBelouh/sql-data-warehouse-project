/*
================================================================================
Purpose:
    Analyze yearly sales performance using revenue (sales_amount) instead of price.

Definition:
    sales_amount = price × quantity

Why this matters:
    Using sales_amount captures both pricing and sales volume, making the analysis
    more representative of actual business performance than price alone.

What this query does:
    1. Aggregates total yearly sales revenue.
    2. Calculates the average sales amount per transaction each year.
    3. Computes a running (cumulative) total of yearly sales to show long-term growth.
    4. Calculates a centered moving average of yearly average sales
       (previous year, current year, next year) to smooth volatility.

Business use cases:
    - Track long-term revenue growth
    - Smooth yearly fluctuations to identify sales trends
    - Support revenue forecasting and performance evaluation
================================================================================
*/

SELECT 
    order_year,
    total_sales,
    SUM(total_sales) OVER(ORDER BY order_year) AS running_sales_total,
    avg_sales,
    ROUND(AVG(avg_sales) OVER(ORDER BY order_year ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 2) AS moving_sales_avg
FROM (
    SELECT
        DATE_TRUNC('year', order_date)::DATE AS order_year,
        SUM(sales_amount) AS total_sales,
        ROUND(AVG(sales_amount), 2) AS avg_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('year', order_date)
) t

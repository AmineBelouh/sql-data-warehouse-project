/*
===============================================================================
Category Sales Contribution Analysis
===============================================================================
Objective:
    Analyze total sales performance by product category and measure each
    category’s contribution to overall company revenue.

What this query does:
    1. Aggregates total sales_amount per product category.
    2. Calculates overall sales across all categories using a window function.
    3. Computes each category’s percentage contribution to total sales.
    4. Orders categories from highest to lowest revenue contribution.

Business Value:
    - Identifies top-performing and underperforming product categories
    - Supports portfolio optimization and resource allocation decisions
    - Helps management understand revenue concentration and risk

Data Sources:
    - gold.fact_sales      : transactional sales data
    - gold.dim_products    : product and category information
===============================================================================
*/

WITH category_sales_cte AS (
    SELECT
        p.category,
        SUM(s.sales_amount) AS category_sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON s.product_key = p.product_key
    GROUP BY p.category
)

SELECT
    *,
    SUM(category_sales) OVER () AS overall_sales,
    CONCAT(
        ROUND(category_sales / SUM(category_sales) OVER () * 100, 2),
        '%'
    ) AS percentage_of_total
FROM category_sales_cte
ORDER BY category_sales DESC;

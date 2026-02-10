/*
===============================================================================
Product Cost Segmentation Analysis
===============================================================================
Objective:
    Segment products based on their unit cost to understand cost distribution
    across the product portfolio.

What this query does:
    1. Classifies products into predefined cost ranges:
        - Above 1000
        - 500 to 1000
        - 100 to 500
        - Below 100
    2. Counts the number of products in each cost segment.
    3. Orders segments by product volume.

Business Value:
    - Helps assess product positioning (premium vs low-cost products)
    - Supports pricing strategy and cost management decisions
    - Useful for inventory and procurement planning

Data Source:
    - gold.dim_products : product master data including cost information
===============================================================================
*/

WITH product_cost_segmentation_cte AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE
            WHEN cost >= 1000 THEN 'Above 1000'
            WHEN cost >= 500 THEN '500 to 1000'
            WHEN cost >= 100 THEN '100 to 500'
            ELSE 'Below 100'
        END AS cost_segments
    FROM gold.dim_products
)

SELECT
    cost_segments,
    COUNT(product_key) AS product_count
FROM product_cost_segmentation_cte
GROUP BY cost_segments
ORDER BY product_count DESC;





/*
===============================================================================
Customer Segmentation by Lifespan and Total Spending
===============================================================================
Objective:
    Segment customers based on their relationship duration (lifespan) and
    total spending to identify high-value and long-term customers.

What this query does:
    1. Calculates customer lifespan using first and last purchase dates.
    2. Converts lifespan into months for standardized comparison.
    3. Aggregates total customer spending.
    4. Segments customers into:
        - VIP     : Long lifespan (≥ 12 months) & high spending
        - Regular : Long lifespan (≥ 12 months) & lower spending
        - New     : Short lifespan (< 12 months)
    5. Counts customers per segment.

Business Value:
    - Identifies high-value customers (VIPs)
    - Supports loyalty programs and retention strategies
    - Helps prioritize marketing and customer engagement efforts

Data Sources:
    - gold.fact_sales      : transactional sales data
    - gold.dim_customers   : customer master data
===============================================================================
*/

WITH customer_spending_lifespan AS (
    SELECT
        c.customer_key,
        MIN(s.order_date) AS first_order,
        MAX(s.order_date) AS last_order,
        AGE(MAX(order_date), MIN(order_date)) AS customer_age,
        (
            DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12
            + DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))
        ) AS lifespan_months,
        SUM(s.sales_amount) AS total_spending
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c
        ON s.customer_key = c.customer_key
    GROUP BY c.customer_key
)

SELECT 
    customer_segments,
    COUNT(customer_key) AS customer_count
FROM (
    SELECT
        customer_key,
        lifespan_months,
        total_spending,
        CASE
            WHEN lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segments
    FROM customer_spending_lifespan
) t
GROUP BY customer_segments
ORDER BY customer_count DESC;

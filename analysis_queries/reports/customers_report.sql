/*
===============================================================================
Customer Reporting View – gold.report_customers
===============================================================================
Objective:
    Create a consolidated customer-level reporting view that combines
    transactional sales data with customer master data to support
    segmentation, behavioral analysis, and KPI reporting.

What this view does:
    1. Extracts detailed sales and customer information at the order level.
    2. Aggregates transactional data to the customer level.
    3. Computes key customer metrics such as:
        - Total orders, products, quantities
        - Total and average spending
        - Customer lifespan (in months)
        - Purchase recency (in months)
    4. Segments customers by:
        - Age groups
        - Behavioral value (VIP / Regular / New)
    5. Provides ready-to-use metrics for dashboards and business analysis.

Key Metrics Included:
    - Total Spending
    - Average Order Value (AOV)
    - Average Monthly Spend
    - Customer Lifespan
    - Recency
    - Order & Product Diversity

Business Value:
    - Enables customer value analysis
    - Supports marketing, CRM, and retention strategies
    - Acts as a reusable semantic layer for BI tools (Power BI, Tableau)

Data Sources:
    - gold.fact_sales      : transactional sales data
    - gold.dim_customers   : customer master data

Output:
    - One row per customer with enriched metrics and segments
===============================================================================
*/

DROP VIEW IF EXISTS gold.report_customers;

CREATE VIEW gold.report_customers AS (
    WITH base_query AS (
        -- ================================================
        -- Base Query: Retrieves core columns from tables
        -- ================================================
        SELECT
            s.order_number,
            s.product_key,
            s.order_date,
            s.sales_amount,
            s.quantity,
            c.customer_key,
            c.customer_number,
            CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
            EXTRACT(YEAR FROM AGE(NOW(), c.birthdate)) AS customer_age
        FROM gold.fact_sales s
        LEFT JOIN gold.dim_customers c 
            ON s.customer_key = c.customer_key
        WHERE s.order_date IS NOT NULL
    ),
    customer_aggregations AS (
        -- ====================================================================
        -- Customer Aggregations: Summarizes key metrics at the customer level
        -- ====================================================================
        SELECT 
            customer_key,
            customer_number,
            customer_name,
            customer_age,
            COUNT(DISTINCT order_number) AS total_orders,
            COUNT(DISTINCT product_key) AS total_products,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_spending,
            MAX(order_date) AS last_order_date,
            (
                DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12
                + DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))
            ) AS lifespan_months
        FROM base_query
        GROUP BY 
            customer_key,
            customer_number,
            customer_name,
            customer_age
    )
    SELECT
        customer_key,
        customer_number,
        customer_name,
        customer_age,
        CASE
            WHEN customer_age > 50 THEN 'Above 50'
            WHEN customer_age > 40 THEN '41 to 50'
            WHEN customer_age > 30 THEN '31 to 40'
            WHEN customer_age > 20 THEN '21 to 30'
            ELSE '20 and below'
        END AS age_segments,
        CASE
            WHEN lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan_months >= 12 AND total_spending < 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segments,
        last_order_date,
        (
            DATE_PART('year', AGE(NOW(), last_order_date)) * 12
            + DATE_PART('month', AGE(NOW(), last_order_date))
        ) AS recency_months,
        lifespan_months,
        total_spending,
        total_orders,
        total_products,
        total_quantity,
        -- Average Order Value
        total_spending / NULLIF(total_orders, 0) AS avg_order_value,
        -- Average Monthly Spend
        total_spending / NULLIF(lifespan_months, 0)::int AS avg_monthly_spend
    FROM customer_aggregations
);

SELECT * FROM gold.report_customers;

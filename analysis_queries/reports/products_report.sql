/*
===============================================================================
Product Reporting View – gold.report_products
===============================================================================
Objective:
    Build a product-level reporting view that consolidates sales transactions
    and product master data to support performance analysis, segmentation,
    and revenue tracking.

What this view does:
    1. Extracts detailed order-line sales data and enriches it with product
       attributes (category, subcategory, cost).
    2. Aggregates transactional data at the product level.
    3. Computes key product KPIs such as:
        - Total sales and quantities sold
        - Number of orders and unique customers
        - Average selling price
        - Product lifespan and recency
    4. Segments products into performance tiers using revenue distribution:
        - High Performers
        - Mid-Range Performers
        - Low Performers
       (based on NTILE(3) over total sales).
    5. Derives normalized metrics for comparison across products:
        - Average order revenue
        - Average monthly revenue

Key Metrics Included:
    - Total Sales
    - Quantity Sold
    - Total Orders
    - Total Customers
    - Average Selling Price
    - Product Lifespan (months)
    - Product Recency (months)
    - Revenue per Order
    - Revenue per Month

Business Value:
    - Identifies top- and low-performing products
    - Supports portfolio optimization and pricing analysis
    - Provides a reusable semantic layer for BI dashboards

Data Sources:
    - gold.fact_sales     : transactional sales data
    - gold.dim_products   : product master data

Output:
    - One row per product with enriched KPIs and performance segments
===============================================================================
*/

DROP VIEW IF EXISTS gold.report_products;

CREATE VIEW gold.report_products AS (
    WITH base_query AS (
        -- ============================================================
        -- Base Query: Order-level sales enriched with product attributes
        -- ============================================================
        SELECT
            s.order_number,
            s.customer_key,
            s.order_date,
            s.quantity,
            s.price,
            s.sales_amount,
            p.product_key,
            p.product_number,
            p.product_name,
            p.category,
            p.subcategory,
            p.cost
        FROM gold.fact_sales s
        LEFT JOIN gold.dim_products p 
            ON s.product_key = p.product_key
        WHERE s.order_date IS NOT NULL
    ),
    product_aggregations AS (
        -- ============================================================
        -- Product Aggregations: Computes KPIs at the product level
        -- ============================================================
        SELECT
            product_key,
            product_number,
            product_name,
            category,
            subcategory,
            cost,
            COUNT(DISTINCT order_number) AS total_orders,
            COUNT(DISTINCT customer_key) AS total_customers,
            SUM(quantity) AS quantity_sold,
            ROUND(AVG(price), 2) AS avg_selling_price,
            SUM(sales_amount) AS total_sales,
            MAX(order_date) AS last_order_date,
            (
                DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12
                + DATE_PART('month', AGE(MAX(order_date), MIN(order_date)))
            ) AS lifespan_months,
            (
                DATE_PART('year', AGE(NOW(), MAX(order_date))) * 12
                + DATE_PART('month', AGE(NOW(), MAX(order_date)))
            ) AS recency_months
        FROM base_query
        GROUP BY
            product_key,
            product_number,
            product_name,
            category,
            subcategory,
            cost
    )
    SELECT
        product_key,
        product_number,
        product_name,
        category,
        subcategory,
        cost,
        total_orders,
        total_customers,
        quantity_sold,
        avg_selling_price,
        total_sales,
        CASE
            WHEN NTILE(3) OVER (ORDER BY total_sales DESC) = 1 THEN 'High Performers'
            WHEN NTILE(3) OVER (ORDER BY total_sales DESC) = 2 THEN 'Mid-Range Performers'
            ELSE 'Low Performers'
        END AS performance_segments,
        last_order_date,
        lifespan_months,
        recency_months,
        -- Average revenue generated per order
        total_sales / NULLIF(total_orders, 0) AS average_order_revenue,
        -- Average monthly revenue over the product lifespan
        total_sales / NULLIF(lifespan_months, 0)::int AS average_monthly_revenue
    FROM product_aggregations
);

SELECT * FROM gold.report_products;

/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer.
===============================================================================
*/

-- ===============================================================================
-- CUSTOMERS DIMENSION
-- ===============================================================================

-- Check for any duplicates in the primary key after the join
select cst_id, count(*) from (
	select 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la on ci.cst_key = la.cid
)t 
group by cst_id
having count(*) > 1


-- data integration
-- we have two columns providing the gender info
select distinct
	ci.cst_gndr,
	ca.gen,
	case
		when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the Master for gender info
		else coalesce(ca.gen, 'n/a')
	end
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la on ci.cst_key = la.cid
order by 1, 2


-- ===============================================================================
-- PRODUCTS DIMENSION
-- ===============================================================================
  
-- Check for any duplicates in the primary key after the join
select prd_key, count(*) from (
	select
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pc.cat,
		pc.subcat,
		pc.maintenance,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt
	from silver.crm_prd_info pn
	left join silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id 
	where pn.prd_end_dt is null -- Get only current data (no historical data)
) t 
group by prd_key
having count(*) > 1

  
-- ===============================================================================
-- SALES FACT
-- ===============================================================================

-- Foreign Key Integrity
select * 
from gold.fact_sales sl
left join gold.dim_customers cu on sl.customer_key = cu.customer_key
left join gold.dim_products pr on sl.product_key = pr.product_key
where cu.customer_key is null or pr.product_key is null

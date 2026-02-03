/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
===============================================================================
*/


-- Check for nulls or duplicates in Primary Key
-- Expectation: no results
select 
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null


-- Check for unwanted spaces
-- Expectation: no results
select
	prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)


-- Data Standardization & Consistency
select distinct prd_line
from silver.crm_prd_info


-- Check for nulls or negative numbers
-- Expectation: no results
select prd_cost
from silver.crm_prd_info
where prd_cost is null or prd_cost < 0


-- Check for invalid date orders
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select * from silver.crm_prd_info


-- Check for invalid dates
select 
	sls_due_dt
from silver.crm_sales_details
where
	sls_due_dt <= 0
	or length(sls_due_dt::text) != 8
	or sls_due_dt > 20250101 
	or sls_due_dt < 19000101


-- Check for invalid date orders
select *
from silver.crm_sales_details
where
	sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


-- Check for data consistency: sales, quantity, price
-- Sales = Quantity * Price
-- Null, zero, negative are not allowed
select distinct
	sls_sales as sls_sales_old,
	sls_quantity,
	sls_price as sls_price_old,
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

select * from silver.crm_sales_details


-- Remove 'NAS' from cid in erp_cust
select 
	cid,
	bdate,
	gen
from silver.erp_cust_az12 
where cid not in (select distinct cst_key from silver.crm_cust_info)


-- identify out-of-range dates
select 
	bdate
from silver.erp_cust_az12 
where bdate < '1924-01-01' or bdate > now()


-- data standardization & consistency
select distinct 
	gen,
	case
		when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M', 'MALE') then 'Male'
		else 'n/a'
	end as gen
from silver.erp_cust_az12 


-- check for unwanted spaces
select * from silver.erp_px_cat_g1v2 
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)


-- Data standardization
select distinct maintenance from silver.erp_px_cat_g1v2

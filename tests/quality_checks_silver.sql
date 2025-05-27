/*
=================================================================================
Quality Checks
=================================================================================
Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across 'silver' schemas. It includes checks for:
  - null or duplicate primary keys
  - unwanted spaces in string fields
  - data standardization and consistency
  - invalid date ranges and orders
  - data consistency between related fields

Usage notes:
  - Run these checks after data loading Silver Layer
  - Investigate and resolve any discrepancies found during the checks
=================================================================================
 */

-- ===================================================================
-- checking 'silver.crm_cust_info'
-- ===================================================================

-- check for nulls or duplicates in primary key
-- expectation: no result
select
    cst_id
    , count(*)
from silver.crm_cust_info
group by cst_id
having count(*) >1 or cst_id is null;

--check for unwanted spaces
--expectation: no result
select cst_firstname
from silver.crm_cust_info
where cst_firstname <> trim(cst_firstname);

--data standardisation & consistency
select distinct cst_gndr
from silver.crm_cust_info;

select distinct cst_marital_status
from silver.crm_cust_info;

-- ===================================================================
-- checking 'silver.crm_prd_info'
-- ===================================================================
-- check for nulls or duplicates in primary key
-- expectation: no result
select
    prd_id
    , count(*)
from silver.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null;

-- check for unwanted spaces
-- expectation: no result
select prd_nm
from silver.crm_prd_info
where prd_nm <> trim(prd_nm);

-- check for nulls or negative numbers
-- expectation: no results
select
    prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- data standardization * consistency
select distinct prd_line
from silver.crm_prd_info;

--check for invalid date orders
select
    *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- ===================================================================
-- checking 'silver.crm_sales_details'
-- ===================================================================
-- check for nulls or duplicates in primary key
-- expectation: no result
select
    *
from silver.crm_sales_details
where sls_ord_num <> trim(sls_ord_num);

--check if prd_key exist in prd_info table & if cust_id exist in cust_info table
--expectation: no result
select
    *
from silver.crm_sales_details
where sls_prd_key not in (select distinct prd_key from silver.crm_prd_info);

select
    *
from silver.crm_sales_details
where sls_cust_id not in (select distinct cst_id from silver.crm_cust_info);

--check for invalid dates
select
    *
from silver.crm_sales_details
where sls_order_dt <=0 or length(sls_order_dt::text) <> 8;

--check for invalid date order
select
    *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

--check for business rules: sales = quantity * price and sales shouldn't be negative, 0 or null
select
    sls_sales
    , sls_quantity
    , sls_price
from silver.crm_sales_details
where sls_sales <> sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price<=0
;

-- ===================================================================
-- checking 'silver.erp_cust_az12'
-- ===================================================================

-- identify out-of-range dates
-- expectation: no result
select
    distinct bdate
from silver.erp_cust_az12
where bdate > now()::date;

--data standardization & consistency
select distinct gen
    , case
        when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
        when upper(trim(gen)) in ('M', 'MALE') then 'Male'
        else 'n/a' end as gen
from silver.erp_cust_az12;

-- ===================================================================
-- checking 'silver.erp_loc_a101'
-- ===================================================================

--data standardization & consistency
select distinct
    case
        when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US', 'USA') then 'United States'
        when trim(cntry) = '' or cntry is null then 'n/a'
    else cntry end as cntry
from silver.erp_loc_a101;

-- ===================================================================
-- checking 'silver.erp_px_cat_g1v2'
-- ===================================================================

 --check for unwanted spaces
select
    *
from silver.erp_px_cat_g1v2
where cat <> trim(cat) or subcat <> trim(subcat) or maintenance <> trim(maintenance);

--data standardization * consistency
select
    distinct maintenance
from silver.erp_px_cat_g1v2

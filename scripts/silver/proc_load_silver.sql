/*
========================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================================
Purpose:
    This stored procedure performs the ETL process to populate the 'silver' schema
      tables from the 'bronze' schema.

    It performs the following actions:
        - truncates silver tables.
        - inserts transformed and cleansed data from bronze into silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    call silver.load_silver();
========================================================================================
*/
create procedure load_silver()
    language plpgsql
as
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    start_time_batch TIMESTAMP;
    end_time_batch TIMESTAMP;

BEGIN
    start_time_batch := NOW();
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '=======================================';

    RAISE NOTICE '---------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    truncate table silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: crm_cust_info';
    insert into silver.crm_cust_info (
        cst_id
        , cst_key
        , cst_firstname
        , cst_lastname
        , cst_marital_status
        , cst_gndr
        , cst_create_date
    )
    select
        cst_id
        , cst_key
        , trim(cst_firstname) as cst_firstname
        , trim(cst_lastname) as cst_lastname
        , case
              when upper(trim(cst_marital_status)) = 'M' then 'Married'
              when upper(trim(cst_marital_status)) = 'S' then 'Single'
              else 'n/a' end as cst_marital_status
        , case
              when upper(trim(cst_gndr)) = 'F' then 'Female'
              when upper(trim(cst_gndr)) = 'M' then 'Male'
              else 'n/a' end as cst_gndr
        , cst_create_date
    from
        (
            select *
                 , row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
            from bronze.crm_cust_info
            where cst_id is not null
        )a
    where flag_last = 1;
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    truncate table silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: crm_prd_info';
    insert into silver.crm_prd_info(
        prd_id
        , cat_id
        , prd_key
        , prd_nm
        , prd_cost
        , prd_line
        , prd_start_dt
        , prd_end_dt)
    select
        prd_id
        , replace(substr(prd_key, 1, 5), '-', '_') as cat_id
        , substr(prd_key, 7, length(prd_key)) as prd_key
        , prd_nm
        , coalesce(prd_cost, 0) as prd_cost
        , case upper(trim(prd_line))
              when 'M' then 'Mountain'
              when 'R' then 'Road'
              when 'S' then 'Other Sales'
              when 'T' then 'Touring'
              else 'n/a' end as prd_line
        , date(prd_start_dt) as prd_start_dt
        , date(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -interval '1' day) as prd_end_dt
    from bronze.crm_prd_info;
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: crm_sales_details';
    truncate table silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: crm_sales_details';
    insert into silver.crm_sales_details(
        sls_ord_num
        , sls_prd_key
        , sls_cust_id
        , sls_order_dt
        , sls_ship_dt
        , sls_due_dt
        , sls_sales
        , sls_quantity
        , sls_price
    )
    select
        sls_ord_num
        , sls_prd_key
        , sls_cust_id
        , case
            when sls_order_dt = 0 or length(sls_order_dt::text) <> 8 then null
            else to_date(sls_order_dt::text, 'yyyymmdd') end as sls_order_dt
        , case
            when sls_ship_dt = 0 or length(sls_ship_dt::text) <> 8 then null
            else to_date(sls_ship_dt::text, 'yyyymmdd') end as sls_ship_dt
        , case
            when sls_due_dt = 0 or length(sls_due_dt::text) <> 8 then null
            else to_date(sls_due_dt::text, 'yyyymmdd') end as sls_due_dt
        , case
            when sls_sales is null or sls_sales <=0 or sls_sales <> sls_quantity * abs(sls_price)
                then sls_quantity * abs(sls_price)
            else sls_sales end as sls_sales
        , sls_quantity
        , case
            when sls_price is null or sls_price <= 0
                then sls_sales / coalesce(sls_quantity, 0)
            else sls_price end as sls_price
        from bronze.crm_sales_details;
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    RAISE NOTICE '---------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: erp_cust_az12';
    truncate table silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: erp_cust_az12';
    insert into silver.erp_cust_az12(cid, bdate, gen)
    select
        case
            when cid like 'NAS%' then substr(cid, 4, length(cid))
            else cid end as cid
        , case
            when bdate > now()::date then null
            else bdate end as bdate
        , case
            when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
            when upper(trim(gen)) in ('M', 'MALE') then 'Male'
            else 'n/a' end as gen
    from bronze.erp_cust_az12;
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: erp_loc_a101';
    truncate table silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: erp_cust_az12';
    insert into silver.erp_loc_a101(cid, cntry)
    select
        replace(cid, '-', '') as cid
        , case
            when trim(cntry) = 'DE' then 'Germany'
            when trim(cntry) in ('US', 'USA') then 'United States'
            when trim(cntry) = '' or cntry is null then 'n/a'
        else cntry end as cntry
    from bronze.erp_loc_a101;
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: erp_px_cat_g1v2';
    truncate table silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: erp_px_cat_g1v2';
    insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    select *
    from bronze.erp_px_cat_g1v2;
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    end_time_batch := NOW();
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Loading Silver Layer is completed';
    RAISE NOTICE ' - Total Load Duration: % seconds', EXTRACT(EPOCH FROM end_time_batch - start_time_batch);
    RAISE NOTICE '=======================================';

EXCEPTION
    WHEN OTHERS THEN
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error Code: %', SQLSTATE;

END;
$$;

alter procedure load_silver() owner to haen;


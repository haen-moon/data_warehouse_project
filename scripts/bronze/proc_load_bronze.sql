/*
=========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=========================================================================================
Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
        - truncates the bronze tables before loading data.
        - uses the COPY command to load data from csv files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    call bronze.load_bronze();
=========================================================================================
 */

create procedure load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=======================================';

    RAISE NOTICE '---------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/Users/haenmoon/DataGripProjects/personal/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/Users/haenmoon/DataGripProjects/personal/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/Users/haenmoon/DataGripProjects/personal/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    RAISE NOTICE '---------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/Users/haenmoon/DataGripProjects/personal/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/Users/haenmoon/DataGripProjects/personal/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    start_time := NOW();
    RAISE NOTICE '>>Truncating Table: erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/haenmoon/DataGripProjects/personal/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>>--------------';

    end_time_batch := NOW();
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Loading Bronze Layer is completed';
    RAISE NOTICE ' - Total Load Duration: % seconds', EXTRACT(EPOCH FROM end_time_batch - start_time_batch);
    RAISE NOTICE '=======================================';

EXCEPTION
    WHEN OTHERS THEN
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error Code: %', SQLSTATE;

END;
$$;

alter procedure load_bronze() owner to haen;


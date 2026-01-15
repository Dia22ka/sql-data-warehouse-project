/*
================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
================================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
================================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS silver.crm_cust_info;
    CREATE TABLE silver.crm_cust_info(
        cst_id              INT,
        cst_key             TEXT,
        cst_firstname       TEXT,
        cst_lastname        TEXT,
        cst_marital_status  TEXT,
        cst_gndr            TEXT,
        cst_create_date     DATE,
        dwh_create_date     DATE DEFAULT CURRENT_DATE
    );

    DROP TABLE IF EXISTS silver.crm_prd_info;
    CREATE TABLE silver.crm_prd_info(
        prd_id          INT,
        cat_id          TEXT,
        prd_key         TEXT,
        prd_nm          TEXT,
        prd_cost        INT,
        prd_line        CHAR(50),
        prd_start_dt    DATE,
        prd_end_dt      DATE,
        dwh_create_date     DATE DEFAULT CURRENT_DATE
    );

    DROP TABLE IF EXISTS silver.crm_sales_details;
    CREATE TABLE silver.crm_sales_details(
        sls_ord_num     TEXT,
        sls_prd_key     TEXT,
        sls_cust_id     INT,
        sls_order_dt    DATE,
        sls_ship_dt     DATE,
        sls_due_dt      DATE,
        sls_sales       INT,
        sls_quantity    INT,
        sls_price       INT,
        dwh_create_date     DATE DEFAULT CURRENT_DATE
    );

    DROP TABLE IF EXISTS silver.erp_loc_a101;
    CREATE TABLE silver.erp_loc_a101(
        cid     TEXT,
        cntry   TEXT,
        dwh_create_date     DATE DEFAULT CURRENT_DATE
    );

    DROP TABLE IF EXISTS silver.erp_cust_az12;
    CREATE TABLE silver.erp_cust_az12(
        cid     TEXT,
        bdate   DATE,
        gen     TEXT,
        dwh_create_date     DATE DEFAULT CURRENT_DATE
    );

    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
    CREATE TABLE silver.erp_px_cat_g1v2(
        id          TEXT,
        cat         TEXT,
        subcat      TEXT,
        maintenance TEXT,
        dwh_create_date     DATE DEFAULT CURRENT_DATE
    );

    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info(
         cst_id,
         cst_key,
         cst_firstname,
         cst_lastname,
         cst_marital_status,
         cst_gndr,
         cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname)AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a'
    END cst_maritial_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a'
    END cst_gndr,
        cst_create_date
    FROM(
    SELECT *,
           row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL)t
    WHERE flag_last = 1;

    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt)

    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5), '-', '_')AS cat_id,
        SUBSTRING(prd_key,7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         ELSE 'n/a'
    END AS prd_line,
        prd_start_dt,
    LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
    FROM bronze.crm_prd_info;

    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price)
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE
            WHEN sls_order_dt <= 0
              OR LENGTH(sls_order_dt::text) != 8
            THEN NULL
            ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,

        CASE
            WHEN sls_ship_dt <= 0
              OR LENGTH(sls_ship_dt::text) != 8
            THEN NULL
            ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,

        CASE
            WHEN sls_due_dt <= 0
              OR LENGTH(sls_due_dt::text) != 8
            THEN NULL
            ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,

    CASE WHEN sls_sales IS NULL
         OR sls_sales <= 0
         OR sls_sales != sls_quantity*ABS(sls_price)
         THEN sls_quantity*ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,

        sls_quantity,

    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales/NULLIF(sls_quantity,0)
         ELSE sls_price
    END AS sls_price
    FROM bronze.crm_sales_details;

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12
        (cid,
         bdate,
         gen)
    SELECT
    CASE WHEN cid LIKE 'NAS%'       -- remove 'NAS' prefix if present
         THEN SUBSTRING(cid, 4, LENGTH(cid))
         ELSE cid
    END AS cid,

    CASE WHEN bdate > CURRENT_DATE
         THEN NULL
         ELSE bdate
    END AS bdate, -- set future birthdates to NULL

    CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'n/a'
    END AS gen -- normalize gender values and handle unknown cases
    FROM bronze.erp_cust_az12;

    TRUNCATE TABLE SILVER.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101
        (cid,
         cntry)
    SELECT
    REPLACE(cid,'-', '')cid,

    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
    END AS cntry  --Normalize and handle missing or blank country codes
    FROM bronze.erp_loc_a101;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
END;
$$;

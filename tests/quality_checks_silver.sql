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

================================================================================
*/

-- =============================================
-- Checking 'silver.crm_cust_info'
-- =============================================

SELECT * FROM silver.crm_cust_info;

--Check for nulls and duplicates in Primary key
--Expectation: no result
SELECT
    cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces
--Expectation: no results

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT * FROM silver.crm_cust_info;

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data standardization & consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- =============================================
-- Checking 'silver.crm_prd_info'
-- =============================================

SELECT * FROM silver.crm_prd_info;

--Check for nulls and duplicates in Primary key
--Expectation: no result
SELECT
    prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 and prd_id IS NULL;

--Check for unwanted spaces
--Expectation: no results
SELECT prd_nm
    FROM silver.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm);

--Chack for nulls or negative numbers
--Expectations: no results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

--Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- =============================================
-- Checking 'silver.crm_sales_details'
-- =============================================

--Check for invalid dates
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0
    OR LENGTH(sls_order_dt::text) != 8
    OR LENGTH(sls_order_dt::text) > 238173193
    OR LENGTH(sls_order_dt::text) < 100928300;

SELECT
sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0
    OR LENGTH(sls_ship_dt::text) !=8;

SELECT
sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
    OR LENGTH(sls_due_dt::text) !=8;

--Check for invalid date orders
--Order date must always be earlier than shipping date or due date

SELECT
    *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--Check data consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity*Price
-- >> Values must not be NULL, negative or zero

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_price*sls_quantity
    OR sls_price IS NULL OR sls_sales IS NULL OR sls_quantity IS NULL
    OR sls_price <= 0 OR sls_sales <= 0 OR sls_quantity <=0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT*FROM silver.crm_sales_details;

-- =============================================
-- Checking 'silver.erp_cust_az12'
-- =============================================

--Identify out of range dates
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;

--Data Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

-- =============================================
-- Checking 'silver.erp_loc_a101'
-- =============================================

--Data standardization & consistency
SELECT DISTINCT
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(cntry)
END AS cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

SELECT*FROM silver.erp_loc_a101;

-- =============================================
-- Checking 'silver.erp_px_cat_g1v2'
-- =============================================

--Check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

--Data standardization & consistency
SELECT DISTINCT
    cat
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;

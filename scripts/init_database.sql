/*
 =====================================================
 Create Database and Schemas
 =====================================================
 Script purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up these schemas within the
    database: 'bronze', 'silver' and 'gold'.

 WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database wil be permanently deleted. Proceed with caution
    and ensure you have proper backups before running the script.
 */

--Create Database 'DataWarehouse'

CREATE DATABASE DataWarehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
    cst_id              INT,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
    prd_id          INT,
    prd_key         TEXT,
    prd_nm          TEXT,
    prd_cost        INT,
    prd_line        CHAR(1),
    prd_start_dt    DATE,
    prd_end_dt      DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num     TEXT,
    sls_prd_key     TEXT,
    sls_cust_id     INT,
    sls_order_dt    INT,
    sls_ship_dt     INT,
    sls_due_dt      INT,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid     TEXT,
    cntry   TEXT
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid     TEXT,
    bdate   DATE,
    gen     TEXT
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1;
CREATE TABLE bronze.erp_px_cat_g1v2(
    id          TEXT,
    cat         TEXT,
    subcat      TEXT,
    maintenance TEXT
);




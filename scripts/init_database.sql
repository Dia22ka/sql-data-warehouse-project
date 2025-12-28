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

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


---4rw9riw9riweofif

===print

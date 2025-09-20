/*

Create database and schemas for Data Warehouse

Purpose: This script creates a database named DataWarehouse and three schemas named bronze, silver, and gold after
checking if the database already exists and dropping it if it does.

warning: This script will drop the existing DataWarehouse database if it exists, resulting in loss of all data in that database 
and its schemas forever.
Ensure you have backups of any important data before running this script.
*/

use master;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;

--Create the database DataWarehouse
CREATE DATABASE DataWarehouse;

--Create the schemas bronze, silver and gold
use DataWarehouse;
GO
create SCHEMA bronze;
go
create SCHEMA silver;
go
create SCHEMA gold;

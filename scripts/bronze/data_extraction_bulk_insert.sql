USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_data
AS
BEGIN
    DECLARE @start_time Datetime,@end_time Datetime;
    BEGIN TRY

        -- LOAD DATA INTO bronze.crm_cust_info TABLE FROM CSV FILE

        SET @start_time = GETDATE(); --monitorring execution time
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        from "C:\Users\mypc\OneDrive\Desktop\data_warehouse\source_crm\cust_info.csv"
        WITH
        (   
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken to load data into bronze.crm_cust_info table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        ---- LOAD DATA INTO bronze.crm_prd_info TABLE FROM CSV FILE

        SET @start_time = GETDATE();
        
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        from "C:\Users\mypc\OneDrive\Desktop\data_warehouse\source_crm\prd_info.csv"
        WITH
        (   
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken to load data into bronze.crm_prd_info table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        ---- LOAD DATA INTO bronze.crm_sales_details TABLE FROM CSV FILE

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        from "C:\Users\mypc\OneDrive\Desktop\data_warehouse\source_crm\sales_details.csv"
        WITH
        (   
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load data into bronze.crm_sales_details table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        --LOAD DATA INTO bronze.erp_CUST_AZ12 TABLE FROM CSV FILE

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_CUST_AZ12;

        BULK INSERT bronze.erp_CUST_AZ12
        from "C:\Users\mypc\OneDrive\Desktop\data_warehouse\source_erp\CUST_AZ12.csv"
        WITH
        (   
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken to load data into bronze.erp_CUST_AZ12 table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        --LOAD DATA INTO bronze.erp_LOC_A101 TABLE FROM CSV FILE

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_LOC_A101;

        BULK INSERT bronze.erp_LOC_A101
        from "C:\Users\mypc\OneDrive\Desktop\data_warehouse\source_erp\LOC_A101.csv"
        WITH
        (   
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken to load data into bronze.erp_LOC_A101 table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
                
        --LOAD DATA INTO bronze.erp_PX_CAT_G1V2 TABLE FROM CSV FILE
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

        BULK INSERT bronze.erp_PX_CAT_G1V2
        from "C:\Users\mypc\OneDrive\Desktop\data_warehouse\source_erp\PX_CAT_G1V2.csv"
        WITH
        (   
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            
        );

        SET @end_time = GETDATE();
        PRINT 'Time taken to load data into bronze.erp_PX_CAT_G1V2 table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT 'Error Occurred during BULK INSERT operation: ' + ERROR_MESSAGE() + 
                    ' Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
    END CATCH
END;

EXEC bronze.load_data;

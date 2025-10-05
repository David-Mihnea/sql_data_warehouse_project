use datawarehouse;
GO
-- Insert data into silver layer tables from bronze layer with necessary transformations and cleansing
create or alter PROCEDURE silver.insert_silver_data
AS
BEGIN
    DECLARE @start_time Datetime,@end_time Datetime;
    BEGIN TRY
        SET @start_time = GETDATE();
        
    TRUNCATE TABLE silver.crm_cust_info;
    insert into silver.crm_cust_info (cst_id, 
                                    cst_key, 
                                    cst_firstname,
                                    cst_lastname, 
                                    cst_gndr, 
                                    cst_marital_status, 
                                    cst_create_date)
    select
        cst_id,
        cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male' 
            when UPPER(TRIM(cst_gndr)) = 'F' then 'Female' 
            else 'n/a' 
            end as cst_gndr, 
        case when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married' 
            when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single' 
            else 'n/a' 
            end as cst_marital_status, 
        cst_create_date
    from (select *,
            ROW_NUMBER() OVER (partition by cst_id ORDER BY cst_create_date  DESC) AS flag_last 
        from bronze.crm_cust_info
        where cst_id is not null     
    ) t where flag_last =1; 

            SET @end_time = GETDATE();
        PRINT 'Time taken to load data into silver.crm_cust_info table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        SET @start_time = GETDATE();
    TRUNCATE TABLE silver.crm_prd_info;
    insert into silver.crm_prd_info (prd_id, 
                                    cat_id,
                                    prd_key, 
                                    prd_nm,
                                    prd_cost, 
                                    prd_line, 
                                    prd_start_dt,
                                    prd_end_dt)
    select 
        prd_id,
        replace(SUBSTRING(prd_key,1,5), '-', '_') as cat_id, 
        substring(prd_key,7,len(prd_key)) as prd_key, 
        prd_nm,
        coalesce(prd_cost, 0) as prd_cost, 
        case when upper(trim(prd_line))= 'M' then 'Mountain'
            when upper(trim(prd_line))= 'R' then 'Road'
            when upper(trim(prd_line))= 'T' then 'Touring'
            when upper(trim(prd_line))= 'S' then 'other Sales'
            else 'n/a' 
        end as prd_line, 
        cast(prd_start_dt as date) as prd_start_dt,
        cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt  
    from bronze.crm_prd_info

            SET @end_time = GETDATE();
        PRINT 'Time taken to load data into silver.crm_prd_info table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        SET @start_time = GETDATE();

    TRUNCATE TABLE silver.crm_sales_details;
    insert into silver.crm_sales_details (sls_ord_num, 
                                        sls_prd_key, 
                                        sls_cust_id, 
                                        sls_order_dt, 
                                        sls_ship_dt, 
                                        sls_due_dt, 
                                        sls_sales, 
                                        sls_quantity, 
                                        sls_price)
    select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt <=0 or len(sls_order_dt) <>8 then null 
        else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt,
        case when sls_ship_dt <=0 or len(sls_ship_dt) <>8 then null 
        else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,
        case when sls_due_dt <=0 or len(sls_due_dt) <>8 then null 
        else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,
        case when sls_sales <= 0 or sls_sales is null  or sls_sales <> abs(sls_price)*sls_quantity then abs(sls_price)*sls_quantity
        else sls_sales 
        end as sls_sales,
        sls_quantity,
        case when sls_price <= 0 or sls_price is null then sls_sales/nullif(sls_quantity, 0)
        else sls_price 
        end as sls_price
    from bronze.crm_sales_details

            SET @end_time = GETDATE();
        PRINT 'Time taken to load data into silver.crm_sales_details table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        SET @start_time = GETDATE();

    TRUNCATE TABLE silver.erp_CUST_AZ12;
    insert into silver.erp_CUST_AZ12 (CID, BDATE, GEN)
    select 
        case when CID like 'NAS%' then substring(CID, 4, len(CID))
            else CID
            end as CID, 
        case when BDATE>GETDATE() then NULL
            else BDATE
            end as BDATE, 
        case when upper(trim(GEN)) in ('F','FEMALE') then 'Female'
            when upper(trim(GEN)) in ('M','MALE') then 'Male'
            else 'n/a'
            end as GEN
    from bronze.erp_CUST_AZ12

            SET @end_time = GETDATE();
        PRINT 'Time taken to load data into silver.erp_CUST_AZ12 table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        SET @start_time = GETDATE();

    TRUNCATE TABLE silver.erp_LOC_A101;
    insert into silver.erp_LOC_A101 (CID, CNTRY)
    select 
        replace(CID, '-', '') as CID,
        case when UPPER(TRIM(CNTRY)) in ('US','USA') then 'United States'
            when UPPER(TRIM(CNTRY)) ='DE' then 'Germany'
            when trim(CNTRY) ='' or cntry is null then 'n/a'
            else trim(CNTRY)
        END as CNTRY
    from bronze.erp_LOC_A101

            SET @end_time = GETDATE();
        PRINT 'Time taken to load data into silver.erp_LOC_A101 table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

        SET @start_time = GETDATE();

    TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
    insert into silver.erp_PX_CAT_G1V2 (id, cat, SUBACT, maintenance)
    select 
        id,
        cat,
        SUBACT,
        maintenance
    from bronze.erp_PX_CAT_G1V2

            SET @end_time = GETDATE();
        PRINT 'Time taken to load data into silver.erp_PX_CAT_G1V2 table: ' +
                CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH

end

exec silver.insert_silver_data;

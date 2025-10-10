/*
    This script creates the Gold layer views in the data warehouse.
    It transforms and integrates data from the Silver layer to create
    dimensional models suitable for analysis and reporting.
    Actions taken:
    1. Dimensional Modeling: Creates dimension and fact tables/views.
    2. Data Integration: Joins and integrates data from multiple Silver layer tables.
    3. Data Enrichment: Adds calculated fields and surrogate keys for better analysis.
*/

use datawarehouse;
go 

create or alter view gold.dim_customers as
select 
    ROW_NUMBER() over (order by ci.cst_id) as customer_key, --surrogate key
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.CNTRY as country,
    ci.cst_marital_status as marital_status,
    CASE when ci.cst_gndr <> 'n/a' then ci.cst_gndr
        else coalesce(ca.GEN, 'n/a')
    END as gender,
    ca.BDATE as birthdate,
    ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_CUST_AZ12 as ca
    on ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 as la
    on ci.cst_key = la.CID;

go

select * from gold.dim_customers;

go

create or alter view gold.dim_products as
select 
    ROW_NUMBER() over (order by pn.prd_start_dt,pn.prd_key) as product_key, -- surrogate key
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.SUBACT as subcategory,
    pc.maintenance,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
 from silver.crm_prd_info as pn
 left join silver.erp_PX_CAT_G1V2 as pc
    on pn.cat_id = pc.id
 where pn.prd_end_dt is null; --- current products only

go
select * from gold.dim_products;
go

create or alter view gold.fact_sales as
select 
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as ship_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
    on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
    on sd.sls_cust_id = cu.customer_id; 

go
select * from gold.fact_sales;

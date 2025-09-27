
/*

*************************************************************************************************
	DDL script for creating gold layer views
*************************************************************************************************
	Purpose:
		
		This script creates all required views in the gold schema 
		(gold layer of the Data Warehouse)

	Notes: 
	
		1.	Before running this script, execute procedure_silver_layer.sql 
			to ensure required schemas are created.

*************************************************************************************************
*/



use SalesDataWarehouse;

GO

CREATE  OR ALTER VIEW gold.dim_customers AS 

	SELECT 
		
		
		cci.cst_id AS customer_id,
		cci.cst_key AS customer_key,
		cci.cst_firstname AS first_name,
		cci.cst_lastname AS last_name,
		CASE 
			WHEN cci.cst_gndr != 'N/A' THEN cci.cst_gndr -- Use CRM gender if available, else ERP gender
			ELSE COALESCE(eca.gen,'N/A')
		END AS gender,
		eca.bdate AS birth_date,
		cci.cst_marital_status AS marital_status,
		ela.cntry AS country,
		cci.cst_create_date AS create_date

	FROM silver.crm_cust_info AS cci

		LEFT JOIN  silver.erp_cust_az12 AS eca
		ON cci.cst_key=eca.cid

		LEFT JOIN silver.erp_loc_a101 AS ela
		ON cci.cst_key = ela.cid


GO

PRINT '-----------------------------------------';
PRINT '>>> View gold.dim_customers created';
PRINT '-----------------------------------------';
GO


CREATE OR ALTER VIEW gold.dim_products AS 

	SELECT 
		cpi.prd_id AS product_id, 
		cpi.prd_key AS product_key,
		cpi.cat_id AS category_id ,
		eci.cat AS category_name,
		eci.subcat AS subcategory_name,
		cpi.prd_nm AS product_name,
		cpi.prd_cost AS product_cost,
		cpi.prd_line AS product_line,
		cpi.prd_start_dt AS product_start_date,
		eci.maintenance AS maintenance

	FROM silver.crm_prd_info AS cpi

		LEFT JOIN silver.erp_px_cat_g1v2 eci
		ON cpi.cat_id = eci.id
		WHERE cpi.prd_end_dt IS NULL -- Removing historical products data


GO

PRINT '-----------------------------------------';
PRINT '>>> View gold.dim_products created';
PRINT '-----------------------------------------';

GO

CREATE OR ALTER VIEW gold.fact_sales AS 

	SELECT  
		
		csd.sls_ord_num AS order_number,
		csd.sls_prd_key AS product_key,
		csd.sls_cust_id AS customer_id,
		csd.sls_order_dt AS order_date,
		csd.sls_ship_dt AS ship_date,
		csd.sls_due_dt AS due_date,
		csd.sls_sales AS sales,
		csd.sls_quantity AS quantity,
		csd.sls_price AS price

	FROM silver.crm_sales_details AS csd  
GO

PRINT '-----------------------------------------';
PRINT '>>> View gold.fact_sales created';
PRINT '-----------------------------------------';

GO

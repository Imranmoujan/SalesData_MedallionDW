/*

*************************************************************************************************
	DDL script for creating silver layer tables
*************************************************************************************************
	Purpose:
		
		This script creates all required tables in the silver schema 
		(silver layer of the Data Warehouse)

	Notes: 
	
		1.	Before running this script, execute procedure_bronze_layer.sql

		2.	If any of the listed tables already exist, they will be dropped and recreated.
			Any existing data will be permanently deleted. 

*************************************************************************************************
*/


USE SalesDataWarehouse;


DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date DATE,
	dh_create_date DATETIME2 DEFAULT GETDATE()
);
PRINT '>>> Table silver.crm_cust_info created';


DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR (50),
	prd_cost FLOAT,
	prd_line VARCHAR(10),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dh_create_date DATETIME2 DEFAULT GETDATE()
);
PRINT '>>> Table silver.crm_prd_info created';


DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dh_create_date DATETIME2 DEFAULT GETDATE()
);
PRINT '>>> Table silver.crm_sales_details created';


DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(10),
	dh_create_date DATETIME2 DEFAULT GETDATE()
);
PRINT '>>> Table silver.erp_cust_az12 created';


DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(20),
	dh_create_date DATETIME2 DEFAULT GETDATE()
);
PRINT '>>> Table silver.erp_loc_a101 created';


DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
	id VARCHAR(10),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(10),
	dh_create_date DATETIME2 DEFAULT GETDATE()
);
PRINT '>>> Table silver.erp_px_cat_g1v2 created';

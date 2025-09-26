
/*

*************************************************************************************************
	DDL Script for Creating Bronze Layer Tables.
*************************************************************************************************
	Purpose:
		
		This script creates all required tables in the Bronze Schema 
		(Bronze Layer of the Data Warehouse)

	Notes: 
	
		1.	Before running this script, execute Init_SalesDataWarehouse.sql 
			to ensure required schemas are created.

		2.	If any of the listed tables already exist, they will be dropped and recreated.
			Any existing data will be permanently deleted. 

*************************************************************************************************
*/





USE SalesDataWarehouse;


DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date DATE
);
PRINT '>>> Table bronze.crm_cust_info created';


DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost FLOAT,
	prd_line VARCHAR(10),
	prd_start_dt DATE,
	prd_end_dt DATE
);
PRINT '>>> Table bronze.crm_prd_info created';


DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
PRINT '>>> Table bronze.crm_sales_details created';


DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(10)
);
PRINT '>>> Table bronze.erp_cust_az12 created';


DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(20)
);
PRINT '>>> Table bronze.erp_loc_a101 created';


DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(10),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(10)
);
PRINT '>>> Table bronze.erp_px_cat_g1v2 created';


/*

*************************************************************************************************
	This script inserts data into silver schema tables.
*************************************************************************************************
	Purpose:
		
		This script clears all existing data in the silver schema tables and inserts new data.

	Notes: 
	
		1.	Before running this script, execute ddl_silver_layer.sql 
			to ensure required tables are created.

		2.	Any existing data will be permanently deleted from the tables before new data is inserted.

*************************************************************************************************
*/




use SalesDataWarehouse;

Go

CREATE OR ALTER PROCEDURE silver.load_silver_data AS

BEGIN 

	PRINT '=======================================================';
	PRINT 'Loading Data in silver Layer';
	PRINT '=======================================================';

	PRINT '-------------------------------------------------------';
	PRINT 'Loading Data From CRM Files';
	PRINT '-------------------------------------------------------';


	DECLARE @start_time DATETIME2,@end_time DATETIME2,@batch_start_time DATETIME2,@batch_end_time DATETIME2

	SET @batch_start_time = GETDATE();

	BEGIN TRY
	
		SET @start_time=GETDATE()
	
		BEGIN TRANSACTION;

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of silver.crm_cust_info';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE silver.crm_cust_info;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in silver.crm_cust_info';
			PRINT '-------------------------------------------------------';

			INSERT INTO silver.crm_cust_info(cst_id,
											cst_key,
											cst_firstname,
											cst_lastname,
											cst_marital_status,
											cst_gndr,
											cst_create_date)
			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname ,
				TRIM(cst_lastname) AS cst_lastname,
				TRIM(UPPER(cst_marital_status)) AS cst_marital_status,
				CASE															-- standardize gender values to 'Male', 'Female', 'Other', or 'N/A'
					WHEN UPPER(TRIM(cst_gndr)) IN('FEMALE','F') THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) IN ('MALE','M') THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) IN ('OTHER','O') THEN 'Other'
					ELSE 'N/A'
				END AS cst_gndr,
				cst_create_date
			FROM(
				SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag FROM 
				bronze.crm_cust_info)t WHERE flag = 1 AND cst_id IS NOT NULL;


		COMMIT TRANSACTION;
		SET @end_time=GETDATE()

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in silver.crm_cust_info:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY

	BEGIN CATCH

		ROLLBACK TRANSACTION;

		PRINT '---------------------------------------------------------------';
		PRINT 'Error occurred, transaction rolled back!';
		PRINT 'Error Occurred During Inserting Data : silver.crm_cust_info';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------';

	END CATCH


	BEGIN TRY

		SET @start_time=GETDATE()
	
		BEGIN TRANSACTION;

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of silver.crm_prd_info';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE silver.crm_prd_info;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in silver.crm_prd_info';
			PRINT '-------------------------------------------------------';

			INSERT INTO silver.crm_prd_info(prd_id,
											cat_id,
											prd_key,
											prd_nm,
											prd_cost,
											prd_line,
											prd_start_dt,
											prd_end_dt)
			SELECT
					prd_id,
					REPLACE(SUBSTRING(TRIM(prd_key),1,5),'-','_') AS cat_id, -- extracting substring and derive new column
					SUBSTRING(TRIM(prd_key),7,LEN(prd_key)) AS prd_key,
					TRIM(prd_nm) AS prd_nm,
					prd_cost,
					CASE
						WHEN TRIM(prd_line)='' or prd_line IS NULL THEN 'N/A'
						ELSE prd_line
						END AS prd_line,
					prd_start_dt,
					DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt  FROM bronze.crm_prd_info


		COMMIT TRANSACTION;

		SET @end_time=GETDATE()

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in silver.crm_prd_info: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY

	BEGIN CATCH
		
		ROLLBACK TRANSACTION;

		PRINT '---------------------------------------------------------------';
		PRINT 'Error occurred, transaction rolled back!';
		PRINT 'Error Occurred During Inserting Data : silver.crm_prd_info';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------';

	END CATCH


	BEGIN TRY

		SET @start_time=GETDATE()
	
		BEGIN TRANSACTION;

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of silver.crm_sales_details';
			PRINT '-------------------------------------------------------'

			TRUNCATE TABLE silver.crm_sales_details;
	
			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in silver.crm_sales_details';
			PRINT '-------------------------------------------------------';

			INSERT INTO silver.crm_sales_details(
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						sls_order_dt,
						sls_ship_dt,
						sls_due_dt,
						sls_sales,
					    sls_quantity,
					    sls_price)

			SELECT sls_ord_num,
				   sls_prd_key,
				   sls_cust_id,
				   CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8  THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
						END AS sls_order_dt,

				   CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)!=8  THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
						END AS sls_ship_dt,

				   CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8  THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
						END AS sls_due_dt,

				   CASE WHEN sls_sales IS NULL OR sls_sales <=0 or (sls_sales!= ABS(sls_price)* sls_quantity)
						THEN ABS(sls_price)* sls_quantity
						ELSE sls_sales
						END AS sls_sales,
				   sls_quantity,
				   CASE WHEN sls_price = 0 OR sls_sales IS NULL  THEN sls_sales / NULLIF(sls_quantity,0)
						 ELSE ABS(sls_price)
						 END AS sls_price

			FROM bronze.crm_sales_details

		COMMIT TRANSACTION;

		SET @end_time=GETDATE()

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in silver.crm_sales_details:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY

	BEGIN CATCH

		ROLLBACK TRANSACTION;

		PRINT '---------------------------------------------------------------';
		PRINT 'Error occurred, transaction rolled back!';
		PRINT 'Error Occurred During Inserting Data : silver.crm_sales_details';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------';

	END CATCH

	PRINT '-------------------------------------------------------';
	PRINT 'Loading Data From ERP Files';
	PRINT '-------------------------------------------------------';

	BEGIN TRY

		SET @start_time=GETDATE()
	
		BEGIN TRANSACTION;

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of silver.erp_cust_az12';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE silver.erp_cust_az12;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in silver.erp_cust_az12';
			PRINT '-------------------------------------------------------';

			INSERT  INTO  silver.erp_cust_az12(
							cid,
							bdate,
							gen)
			SELECT 
				CASE 
					WHEN TRIM(UPPER(cid)) LIKE 'NAS%' THEN SUBSTRING(TRIM(cid),4,len(cid))
					ELSE cid
				END AS cid,
				CASE
					WHEN DATEDIFF(YEAR,bdate,GETDATE()) <= 0 THEN NULL
					WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
					END AS bdate,
				CASE 
					WHEN UPPER(TRIM(gen)) IN('FEMALE','F') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('MALE','M') THEN 'Male'
					WHEN UPPER(TRIM(gen)) IN ('OTHER','O') THEN 'Other'
					ELSE 'N/A'
				END AS gen
				FROM bronze.erp_cust_az12 ;

		COMMIT TRANSACTION;

		SET @end_time=GETDATE()

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in silver.erp_cust_az12:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY
	BEGIN CATCH

		ROLLBACK TRANSACTION;

		PRINT '---------------------------------------------------------------';
		PRINT 'Error occurred, transaction rolled back!';
		PRINT 'Error Occurred During Inserting Data : silver.erp_cust_az12';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------';

	END CATCH


	BEGIN TRY

		SET @start_time=GETDATE()
	
		BEGIN TRANSACTION;

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of silver.erp_loc_a101';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE silver.erp_loc_a101;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in silver.erp_loc_a101';
			PRINT '-------------------------------------------------------';

			INSERT INTO silver.erp_loc_a101(
							cid,
							cntry)

			SELECT 
				UPPER(TRIM(REPLACE(cid,'-',''))) AS cid,
				CASE
					WHEN UPPER(TRIM(cntry)) IN ('US','UNITED STATES','USA') THEN 'US'
					WHEN UPPER(TRIM(cntry)) IN ('CANADA','CA') THEN 'CA'
					WHEN UPPER(TRIM(cntry)) IN ('GERMANY','DE') THEN 'DE'
					WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM','UK') THEN 'UK'
					WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA','AUS','AU') THEN 'AU'
					WHEN UPPER(TRIM(cntry)) IN ('FRANCE','FR') THEN 'FR'
					WHEN TRIM(cntry) ='' or cntry IS NULL THEN 'N/A'
					ELSE TRIM(cntry)
				END AS cntry
			FROM bronze.erp_loc_a101;

		COMMIT TRANSACTION;

		SET @end_time=GETDATE()

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in silver.erp_loc_a101:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY
	BEGIN CATCH

		ROLLBACK TRANSACTION;
		PRINT '---------------------------------------------------------------';
		PRINT 'Error occurred, transaction rolled back!';
		PRINT 'Error Occurred During Inserting Data : silver.erp_loc_a101';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------';

	END CATCH
	

	BEGIN TRY

		SET @start_time=GETDATE()
	
		BEGIN TRANSACTION;

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of silver.erp_px_cat_g1v2';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE silver.erp_px_cat_g1v2;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in silver.erp_px_cat_g1v2';
			PRINT '-------------------------------------------------------';

			INSERT INTO silver.erp_px_cat_g1v2(
						id,
						cat,
						subcat,
						maintenance)

			SELECT 
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2

		COMMIT TRANSACTION;

		SET @end_time=GETDATE()

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in silver.erp_px_cat_g1v2:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY

	BEGIN CATCH

		ROLLBACK TRANSACTION;

		PRINT '---------------------------------------------------------------';
		PRINT 'Error occurred, transaction rolled back!';
		PRINT 'Error Occurred During Inserting Data : silver.erp_px_cat_g1v2';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------------------------';

	END CATCH



	SET @batch_end_time = GETDATE();

	PRINT 'Loading data completed. Time required to load data in silver layer is '+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+' second';

END;

GO

EXEC silver.load_silver_data

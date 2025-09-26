
/*

*************************************************************************************************
	This script inserts data into Bronze schema tables.
*************************************************************************************************
	Purpose:
		
		This script clears all existing data in the Bronze schema tables and inserts new data.

	Notes: 
	
		1.	Before running this script, execute ddl_bronze_layer.sql 
			to ensure required tables are created.

		2.	Any existing data will be permanently deleted from the tables before new data is inserted.

*************************************************************************************************
*/



USE SalesDataWarehouse;

GO

CREATE OR ALTER PROCEDURE bronze.load_bronze_data AS

BEGIN

	PRINT '=======================================================';
	PRINT 'Loading Data in Bronze Layer';
	PRINT '=======================================================';

	PRINT '-------------------------------------------------------';
	PRINT 'Loading Data From CRM Files';
	PRINT '-------------------------------------------------------';

	DECLARE @start_time DATETIME ,@end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	SET @batch_start_time=GETDATE();



	BEGIN TRY

		SET @start_time = GETDATE();
		BEGIN TRANSACTION

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of bronze.crm_cust_info';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE bronze.crm_cust_info;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in bronze.crm_cust_info';
			PRINT '-------------------------------------------------------';

			BULK INSERT  bronze.crm_cust_info FROM 'D:\SQL_Project\salesDB\Dataset\source_crm\cust_info.csv'
			WITH (

				FIRSTROW = 2,
				ROWTERMINATOR = '\n',
				FIELDTERMINATOR= ',',
				TABLOCK
			);
		COMMIT TRANSACTION
		SET @end_time = GETDATE();

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in bronze.crm_cust_info:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';


	END TRY
	BEGIN CATCH
	
		ROLLBACK TRANSACTION;
		PRINT '-------------------------------------------------------';
		PRINT 'Error Occurred : bronze.crm_cust_info';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State : '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------';

	END CATCH


	BEGIN TRY

		SET @start_time = GETDATE();
		BEGIN TRANSACTION

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of bronze.crm_prd_info';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE bronze.crm_prd_info;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in bronze.crm_prd_info';
			PRINT '-------------------------------------------------------';

			BULK INSERT  bronze.crm_prd_info FROM 'D:\SQL_Project\salesDB\Dataset\source_crm\prd_info.csv'
			WITH (

				FIRSTROW = 2,
				ROWTERMINATOR = '\n',
				FIELDTERMINATOR= ',',
				TABLOCK

					);

		COMMIT TRANSACTION
		SET @end_time = GETDATE();

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in bronze.crm_prd_info: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY

	BEGIN CATCH
	
		ROLLBACK TRANSACTION
		PRINT '-------------------------------------------------------';
		PRINT 'Error Occurred : bronze.crm_prd_info';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State : '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------';

	END CATCH


	BEGIN TRY

		SET @start_time = GETDATE();
		BEGIN TRANSACTION

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of bronze.crm_sales_details';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE bronze.crm_sales_details;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in bronze.crm_sales_details';
			PRINT '-------------------------------------------------------';

			BULK INSERT  bronze.crm_sales_details FROM 'D:\SQL_Project\salesDB\Dataset\source_crm\sales_details.csv'
			WITH (

				FIRSTROW = 2,
				ROWTERMINATOR = '\n',
				FIELDTERMINATOR= ',',
				TABLOCK
			);


		COMMIT TRANSACTION
		SET @end_time = GETDATE();

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in bronze.crm_sales_details:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY
	BEGIN CATCH
	
		ROLLBACK TRANSACTION
		PRINT '-------------------------------------------------------';
		PRINT 'Error Occurred : bronze.crm_sales_details';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State : '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------';

	END CATCH


	PRINT '-------------------------------------------------------';
	PRINT 'Loading Data From ERP Files';
	PRINT '-------------------------------------------------------';


	BEGIN TRY

		SET @start_time = GETDATE();
		BEGIN TRANSACTION

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of bronze.erp_cust_az12';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE bronze.erp_cust_az12;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in bronze.erp_cust_az12';
			PRINT '-------------------------------------------------------';

			BULK INSERT  bronze.erp_cust_az12 FROM 'D:\SQL_Project\salesDB\Dataset\source_erp\CUST_AZ12.csv'
			WITH (

				FIRSTROW = 2,
				ROWTERMINATOR = '\n',
				FIELDTERMINATOR= ',',
				TABLOCK
			);
		COMMIT TRANSACTION
		SET @end_time = GETDATE();

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in bronze.erp_cust_az12:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY
	BEGIN CATCH
	
		ROLLBACK TRANSACTION
		PRINT '-------------------------------------------------------';
		PRINT 'Error Occurred : bronze.erp_cust_az12';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State : '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------';

	END CATCH



	BEGIN TRY

		SET @start_time = GETDATE();
		BEGIN TRANSACTION

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of bronze.erp_loc_a101';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE bronze.erp_loc_a101;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in bronze.erp_loc_a101';
			PRINT '-------------------------------------------------------';

			BULK INSERT  bronze.erp_loc_a101 FROM 'D:\SQL_Project\salesDB\Dataset\source_erp\LOC_A101.csv'
			WITH (

				FIRSTROW = 2,
				ROWTERMINATOR = '\n',
				FIELDTERMINATOR= ',',
				TABLOCK
			);

		COMMIT TRANSACTION
		SET @end_time = GETDATE();

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in bronze.erp_loc_a101:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';

	END TRY
	BEGIN CATCH
	
		ROLLBACK TRANSACTION
		PRINT '-------------------------------------------------------';
		PRINT 'Error Occurred : bronze.erp_loc_a101';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State : '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------';

	END CATCH


	BEGIN TRY

		SET @start_time = GETDATE();
		BEGIN TRANSACTION

			PRINT '-------------------------------------------------------';
			PRINT 'Clearing Data of bronze.erp_px_cat_g1v2';
			PRINT '-------------------------------------------------------';

			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			PRINT '-------------------------------------------------------';
			PRINT 'Loading Data in bronze.erp_px_cat_g1v2';
			PRINT '-------------------------------------------------------';

			BULK INSERT  bronze.erp_px_cat_g1v2 FROM 'D:\SQL_Project\salesDB\Dataset\source_erp\PX_CAT_G1V2.csv'
			WITH (

				FIRSTROW = 2,
				ROWTERMINATOR = '\n',
				FIELDTERMINATOR= ',',
				TABLOCK
			);

		COMMIT TRANSACTION
		SET @end_time = GETDATE();

		PRINT '-------------------------------------------------------';
		PRINT 'Loading Data Completed. Time Required to Load Data in bronze.erp_px_cat_g1v2:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' second';
		PRINT '-------------------------------------------------------';


	END TRY
	BEGIN CATCH
	
		ROLLBACK TRANSACTION
		PRINT '-------------------------------------------------------';
		PRINT 'Error Occurred : bronze.erp_px_cat_g1v2';
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + ERROR_MESSAGE();
		PRINT 'Error State : '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------';

	END CATCH

	SET @batch_end_time=GETDATE();

	PRINT 'Loading data completed. Time required to load data in bronze layer is '+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+' second';


END

GO

EXEC bronze.load_bronze_data;

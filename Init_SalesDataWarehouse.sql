
	/*

	*************************************************************************************************
		DDL Script for Creating Data Warehouse.
	*************************************************************************************************
		Purpose:
		
			This script creates Data Warehouse and all required schema for this Data Warehouse.

		Notes: 
	
			If the SalesDataWarehouse database already exists, it will be dropped and recreated.
			Any existing data will be permanently deleted. 

	*************************************************************************************************
	*/

	USE master;

	GO

	--Drop and recreate the database 
	DROP DATABASE IF EXISTS SalesDataWarehouse;

	GO


	CREATE DATABASE SalesDataWarehouse;

	GO

	PRINT '>>> Database SalesDataWarehouse created successfully';

	-- Switch to the new database
	USE SalesDataWarehouse;

	GO

	-- Create required schema

	CREATE SCHEMA bronze;

	GO
	PRINT '>>> Schema bronze created';

	GO

	CREATE SCHEMA silver;

	GO
	PRINT '>>> Schema silver created';

	GO

	CREATE SCHEMA gold;

	GO
	PRINT '>>> Schema gold created';

	PRINT '>>> Data Warehouse initialization completed successfully';

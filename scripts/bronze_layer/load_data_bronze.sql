/*
===============================================================================
DDL Script: Load & Truncate data into Bronze Tables
===============================================================================
Script Purpose:
    This script define how to load and truncate data  in the 'bronze' schema, truncate existing records in tables  
    if they already exist.
	  Define the Loading time duration and error.
===============================================================================
*/

--USE DataWarehouse;
-- INSERT DATA INTO TABLE
--USE DataWarehouse

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME , @end_time DATETIME;
		PRINT('==============================================================================');
		PRINT('Loading Data INTO Bronze Layer');
		PRINT('==============================================================================');

		PRINT('Loading CRM Table');
		PRINT('------------------------------------------------------------------------------');
		
		SET @start_time = GETDATE();
		PRINT('>> TRUNCATE Table: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('>> INSERT Table: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Projects\SQL Data WareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT('------------------------------------------------------------------------------');
		
		PRINT('>> TRUNCATE Table: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;
	
		PRINT('>> INSERT Table: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Projects\SQL Data WareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT('------------------------------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> TRUNCATE Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('>> INSERT Table: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Projects\SQL Data WareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);

		PRINT('------------------------------------------------------------------------------');
		PRINT('Loading ERP Table');
		PRINT('------------------------------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> TRUNCATE Table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('>> INSERT Table: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Projects\SQL Data WareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT('------------------------------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> TRUNCATE Table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('>> INSERT Table: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Projects\SQL Data WareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT('------------------------------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> TRUNCATE Table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('>> INSERT Table: bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Projects\SQL Data WareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT('------------------------------------------------------------------------------');

		END TRY
		BEGIN CATCH
		PRINT('==============================================================================');
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT('==============================================================================');
		END CATCH
END

EXEC bronze.load_bronze 

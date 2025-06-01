/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

PRINT '-------------------------------------------------------------------------------------';

--First to check the data quality,consistency 
--To check whether the primary key is Not Null and Unique

USE DataWarehouse
SELECT * FROM bronze.crm_cust_info;

-- Rules to maintain the data quality 
   -- Remove the short form of any character with full name
   -- Replace all NULL with unknown OR Filled
   -- Check all the data types

-- To check primary key
/*
SELECT 
	cst_id,
	COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR	cst_id IS NULL;

-- Now Pick any record from them and check it
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_id = 29466 OR cst_id =29483;
*/
-- Always Pick current records from data
*/
--USE Datawarehouse
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
		BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME;

		PRINT('==============================================================================');
		PRINT('Loading Data INTO Silver Layer');
		PRINT('==============================================================================');
		PRINT('Loading CRM Table');
		PRINT('------------------------------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info

		PRINT '>> INSERT INTO TABLE silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN cst_marital_status = 'M' THEN 'Mariage'
				 WHEN cst_marital_status = 'S' THEN 'Single'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE WHEN cst_gndr = 'M' THEN 'Male'
				 WHEN cst_gndr = 'F' THEN 'Female'
				 ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank_num
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL )t
		WHERE rank_num = 1;

		SET @end_time = GETDATE();
		PRINT '>> Loding Duraction: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT '-------------------------------------------------------------------------------------';
		/*
		SELECT 
			prd_id,
			COUNT(*) 
		FROM bronze.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 OR	prd_id IS NULL;

		SELECT * FROM bronze.crm_prd_info;

		*/
		-- ADD A key for relationship with px_cat_g1v2 table
		-- ADD A key for relationship with sales_detail table

		SET @start_time = GETDATE()
		PRINT '>> TRUNCATE TABLE silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info; 

		PRINT '>> INSERT INTO TABLE silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_key,
			px_key,
			sis_prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt )
		SELECT
			prd_id,
			prd_key,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS px_key,
			SUBSTRING(prd_key,7,LEN(prd_key))AS sls_prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sale'
				WHEN 'R' THEN 'Road'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT '-------------------------------------------------------------------------------------';
		
		SET @start_time = GETDATE()
		PRINT '>> TRUNCATE TABLE silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> INSERT INTO TABLE silver.crm_sales_details';
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

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt, 
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END  AS sls_ship_dt, 
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
				END AS sls_due_dt, 
			CASE
				WHEN sls_sales IS NULL OR sls_sales = 0 THEN  sls_price/sls_quantity
				ELSE ABS(sls_sales)
			END sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price = 0 THEN sls_sales * ABS(sls_quantity)
				WHEN sls_price <= sls_sales THEN sls_sales * ABS(sls_quantity)
				ELSE ABS(sls_price)
			END sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT '_____________________________________________________________________________________';
		PRINT('Loading ERP Table');
		PRINT '_____________________________________________________________________________________';
		
		SET @start_time = GETDATE()
		PRINT '>> TRUNCATE TABLE silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> INSERT INTO TABLE silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			c_id,
			b_date,
			gender)

		SELECT 
			CASE 
				WHEN c_id LIKE 'NAS%' THEN SUBSTRING(c_id,4,LEN(c_id)) 
				ELSE TRIM(c_id)
			END AS c_id,
			CASE 
				WHEN  b_date > GETDATE() THEN NULL
				ELSE b_date
			END b_date,
			CASE gender
				WHEN 'F' THEN 'Female'
				WHEN 'M' THEN 'Male'
				WHEN 'Male' THEN 'Male'
				WHEN 'Female' THEN 'Female'
				ELSE 'n/a'
			END AS gender
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT '-------------------------------------------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> TRUNCATE TABLE silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> INSERT INTO TABLE silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		c_id,
		country)

		SELECT 
		REPLACE(c_id,'-','') AS c_id,
		CASE 
			WHEN TRIM(country) = 'DE' THEN 'Germany'
			WHEN TRIM(country) = 'USA' OR TRIM(country) = 'US' THEN 'United Kingdom'
			WHEN TRIM(country) IS NULL OR TRIM(country) = '' THEN 'n/a'
			ELSE TRIM(country)
		END AS country
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
		PRINT '-------------------------------------------------------------------------------------';
		
		SET @start_time = GETDATE()
		PRINT '>> TRUNCATE TABLE silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> INSERT INTO TABLE silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)

		SELECT 
		REPLACE(id,'_','-') AS id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE()
		PRINT 'Loading Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
	END TRY
	BEGIN CATCH
		PRINT('==============================================================================');
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT('==============================================================================');
	END CATCH
END

EXEC silver.load_silver;
----------------------------------------------------------------------------------------------

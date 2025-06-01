/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
--use DataWarehouse
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO
  
CREATE VIEW gold.dim_customer AS
SELECT
		ROW_NUMBER() OVER(ORDER BY cu.cst_id) AS customer_key,
		cu.cst_id AS customer_id,
		cu.cst_key AS customer_number,
		cu.cst_firstname AS first_name,
		cu.cst_lastname AS last_name,
		CASE 
		WHEN cu.cst_gndr != 'n/a' THEN cu.cst_gndr
		ELSE COALESCE(az.gender,'n/a')
		END AS gender,
		cu.cst_marital_status AS marital_status,
		lo.country,
		az.b_date AS birth_date,
		cu.cst_create_date AS created_date
FROM silver.crm_cust_info AS cu
LEFT JOIN silver.erp_cust_az12 AS az
ON cu.cst_key = az.c_id
LEFT JOIN silver.erp_loc_a101 AS lo
ON cu.cst_key = lo.c_id
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
  
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt,pr.prd_id) AS product_key,
	pr.prd_id AS product_id,
	pr.sis_prd_key AS product_number,
	pr.prd_nm AS product_name,
	cat.id AS category_id,
	cat.cat AS category,
	cat.subcat AS subcategory,
	cat.maintenance AS maintenance,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pr
LEFT JOIN silver.erp_px_cat_g1v2 AS cat
ON pr.px_key = cat.id
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
CREATE VIEW gold.fact_sales AS
SELECT 
	sa.sls_ord_num,
	sa.sls_order_dt,
	sa.sls_ship_dt,
	sa.sls_due_dt,
	sa.sls_sales,
	sa.sls_quantity,
	sa.sls_price,
	cu.customer_key,
	pr.product_key
FROM silver.crm_sales_details AS sa
LEFT JOIN gold.dim_customer AS cu
ON sa.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products AS pr
ON sa.sls_prd_key = pr.product_number
GO

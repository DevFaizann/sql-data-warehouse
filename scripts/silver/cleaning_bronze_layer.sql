-- check for nulls or duplicates in primary key
-- this code checks if any primary key is repeated or if there is something null. since we received multiple repeated cst_id. here's the fix

SELECT cst_id,
COUNT(*) 
FROM bronze.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- WE GOT THE ROWS WHERE THE ID WAS REPEATED. WE WILL KEEP THE MOST FRESH ONE

SELECT *

FROM bronze.crm_cust_info

WHERE cst_id = 29466


-- ROW_NUMBER() assigns a unique number to each row
-- Used  ORDER BY to sort the data based on the creation data descending order, hence fresh one on top
-- WHEN ROW_NUMBER() FUNC USED WITH OVER(), ITS CALLED WINDOW FUNCTION
-- for the specific task of generating that flag_last column which numbers the rows within each customer's group based on their creation date, the OVER() clause is essential.

SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466


-- with this we have only unique data with no NULL values

SELECT 
* 
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1


-- Second quality check, unneccesary spaces in string values
-- if the original value is not equal to the value after trimming, it means there are spaces

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- Cleaned the strings data with spaces 

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1


-- DATA STANDARDIZATION AND CONSISTENCY

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Solution

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1


-- quality check for silver layer

SELECT cst_id,
COUNT(*) 
FROM silver.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- crm product info table

SELECT 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


-- creating new coloumn out of existing coloumn
-- in the table erp, there is '_' instead of '-'
-- so replace that with _ 
-- TO CHECK THE DATA NOT AVAILABLE IN THE ERP TABLE (optional to check)
-- WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN (SELECT distinct id from bronze.erp_px_cat_g1v2) 

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,

/*
-- alternate method
CASE UPPER(TRIM(prd_line)) 
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
*/
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


-- check for duplicates or null

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- UNNECCESSARY SPACE CHECK
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- NEGATIVE NUMBER OR NULL CHECK
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL 


-- check for invalid dates. the end date should not be earlier than the start date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt 

-- solution for wrong start and end date

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

/* 
LEAD(prd_start_dt): This function retrieves the value of the prd_start_dt column from the next row in the ordered partition.
OVER (PARTITION BY prd_key ...): This divides the rows into partitions based on the prd_key. So, the LEAD() function will operate independently for each unique prd_key.
*/

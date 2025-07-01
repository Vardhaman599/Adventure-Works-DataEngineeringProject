-- ========================================
-- CREATE VIEW: gold.calendar
-- ========================================
CREATE VIEW gold.calendar AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) AS data;

-- ========================================
-- CREATE VIEW: gold.customers
-- ========================================
CREATE VIEW gold.customers AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) AS data;

-- ========================================
-- CREATE VIEW: gold.products
-- ========================================
CREATE VIEW gold.products AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) AS data;

-- ========================================
-- DROP & RE-CREATE VIEW: gold.returns
-- ========================================
DROP VIEW IF EXISTS gold.returns;
GO

CREATE VIEW gold.returns AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS data;
GO

-- ========================================
-- CREATE VIEW: gold.sales
-- ========================================
CREATE VIEW gold.sales AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) AS data;

-- ========================================
-- CREATE VIEW: gold.subcat
-- ========================================
CREATE VIEW gold.subcat AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_SubCategories/',
    FORMAT = 'PARQUET'
) AS data;

-- ========================================
-- CREATE VIEW: gold.territories
-- ========================================
CREATE VIEW gold.territories AS 
SELECT * 
FROM OPENROWSET(
    BULK 'https://<datalake_url>/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS data;

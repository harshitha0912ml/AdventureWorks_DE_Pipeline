CREATE VIEW gold.Ccalendar
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://adventureworks0912.blob.core.windows.net/adventuresilver/AdventureWorks_Calendar/',
            FORMAT = 'PARQUET'
        ) as QUER1;
--------------------
--customers view---
--------------------
CREATE VIEW gold.customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://adventureworks0912.blob.core.windows.net/adventuresilver/AdventureWorks_Customers/',
            FORMAT = 'PARQUET'
        ) as QUER1;

---------------------
---products view----
---------------------
CREATE VIEW gold.products
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://adventureworks0912.blob.core.windows.net/adventuresilver/AdventureWorks_products/',
            FORMAT = 'PARQUET'
        ) as QUER1;


------------------------
---Product SubCategories---
------------------------
CREATE VIEW gold.subcat
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://adventureworks0912.blob.core.windows.net/adventuresilver/AdventureWorks_productsubcategories/',
            FORMAT = 'PARQUET'
        ) as QUER1;

---------------------------------
---RETURNS TABLE---------------
---------------------------------
CREATE VIEW gold.returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://adventureworks0912.blob.core.windows.net/adventuresilver/AdventureWorks_returns/',
            FORMAT = 'PARQUET'
        ) as QUER1;


-----------------------------
----SALES TABLE--------------
-----------------------------
CREATE VIEW gold.sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://adventureworks0912.blob.core.windows.net/adventuresilver/AdventureWorks_sales/',
            FORMAT = 'PARQUET'
        ) as QUER1;

        

CREATE DATABASE SCOPED CREDENTIAL cred_harshitha
WITH
  IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE awproject_silver
WITH
(
    LOCATION = 'https://adventureworks0912.blob.core.windows.net/adventuresilver',
    CREDENTIAL = cred_harshitha

);

CREATE EXTERNAL DATA SOURCE awproject_gold_1
WITH
(
    LOCATION = 'https://adventureworks0912.blob.core.windows.net/adventuregold',
    CREDENTIAL = cred_harshitha

);


CREATE EXTERNAL FILE FORMAT parquet_format
WITH
(
  FORMAT_TYPE = PARQUET
);


-------------------CREATING EXTERNAL TABLES----------------------------------

CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
  LOCATION = 'extcustomers',
  DATA_SOURCE = awproject_gold_1,
  FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.customers;

--- CALENDER GOLD---
CREATE EXTERNAL TABLE gold.extcalender
WITH
(
  LOCATION = 'extcalender',
  DATA_SOURCE = awproject_gold_1,
  FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.calendar;

---PRODUCTS GOLD---

CREATE EXTERNAL TABLE gold.extproducts
WITH
(
  LOCATION = 'extproducts',
  DATA_SOURCE = awproject_gold_1,
  FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.products;

--RETURNS GOLD--
CREATE EXTERNAL TABLE gold.extreturns
WITH
(
  LOCATION = 'extreturns',
  DATA_SOURCE = awproject_gold_1,
  FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.returns;

--RETURNS SUBCAT--

CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
  LOCATION = 'extsubcat',
  DATA_SOURCE = awproject_gold_1,
  FILE_FORMAT = parquet_format
)
AS SELECT * FROM gold.subcat;




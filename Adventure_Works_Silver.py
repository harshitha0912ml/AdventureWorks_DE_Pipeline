# Databricks notebook source
# MAGIC %md
# MAGIC DATABRICKS SILVER LAYER DATA FOR ADVENTURE WORKS PROJECT
# MAGIC **DATA ACCESS USING SPARK.CONF.SET**

# COMMAND ----------

#### WRITE YOUR SOURCE FILE CONFIG HERE ###

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA LOADING**

# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/")


# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/")



# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/")

# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/")

# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads")

# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/")

# COMMAND ----------

dbutils.fs.ls("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/")

# COMMAND ----------

df_calenderdata = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Calendar.csv/")

# COMMAND ----------

df_calenderdata.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **ALL DATASETS READ**

# COMMAND ----------

df_customers = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Customers.csv/")

df_product_categories = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Product_Categories.csv/")

df_product_subcategories = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Product_Subcategories.csv/")

df_products = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Products.csv/")

df_returns = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Returns.csv/")

df_sales_2015 = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Sales_2015.csv/")

df_sales_2016 = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Sales_2016.csv/")

df_sales_2017 = spark.read.format('csv').option("header",True).option("inferschema",True).load("abfss://adventurebronze@adventureworks0912.dfs.core.windows.net/harshitha0912ml/AdventureWorks_DE_Pipeline/refs/heads/main/Adventureworks_raw_Data/AdventureWorks_Sales_2017.csv/")


# COMMAND ----------

# MAGIC %md
# MAGIC **DATA TRANSFORM**

# COMMAND ----------

df_calenderdata.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **TRANSFORM TO MONTH DATA YEAR**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_calenderdata = df_calenderdata.withColumn('Month',month(col('Date')))\
            .withColumn('Year',year(col('Date')))

# COMMAND ----------

df_calenderdata.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **PUSHING THIS DATA TO SILVER LAYER**

# COMMAND ----------

df_calenderdata.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://adventuresilver@adventureworks0912.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA TRANSFORMATIONS FOR CUSTOMERS**

# COMMAND ----------

df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **creating full name**

# COMMAND ----------

df_customers = df_customers.withColumn('fullName',concat_ws(' ',col('Prefix'),col('FirstName'),col('lastName')))

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://adventuresilver@adventureworks0912.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **PRODUCT SUBCATEGORIES**

# COMMAND ----------

df_product_subcategories.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://adventuresilver@adventureworks0912.dfs.core.windows.net/AdventureWorks_productsubcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **PRODUCTS DF**

# COMMAND ----------

df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **fetching the SKU category and first word of product category** using SPLIT FUNCTION

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_products.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://adventuresilver@adventureworks0912.dfs.core.windows.net/AdventureWorks_products")\
            .save()

# COMMAND ----------

df_returns.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://adventuresilver@adventureworks0912.dfs.core.windows.net/AdventureWorks_returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **SALES DATA**

# COMMAND ----------

df_sales = df_sales_2015.union(df_sales_2016).union(df_sales_2017)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))


# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://adventuresilver@adventureworks0912.dfs.core.windows.net/AdventureWorks_sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **SALES_ANALYSIS** : TOTAL NUMBER OF SALES PER DAY

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.groupBy("Occupation", "EducationLevel") \
    .count() \
    .orderBy("count", ascending=False) \
    .display()


# COMMAND ----------

df_customers.groupBy("Gender").count().display()


# COMMAND ----------

from pyspark.sql.functions import year, current_date

df_age = df_customers.withColumn("Age", year(current_date()) - year("BirthDate"))
df_age.select("Age").display()


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# Clean and cast AnnualIncome to Integer
df_customers_clean = df_customers.withColumn(
    "AnnualIncome",
    regexp_replace("AnnualIncome", "[$,]", "").cast("int")
)

# Group by Occupation and compute average income
df_customers_clean.groupBy("Occupation") \
    .agg({"AnnualIncome": "avg"}) \
    .withColumnRenamed("avg(AnnualIncome)", "AvgIncome") \
    .orderBy("AvgIncome", ascending=False) \
    .display()


# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_joined = df_sales.join(
    df_customers,
    on="CustomerKey",
    how="inner"  # or "left" if you want all sales, even unmatched ones
)

# COMMAND ----------

df_joined.groupBy("Occupation") \
    .sum("OrderQuantity") \
    .withColumnRenamed("sum(OrderQuantity)", "TotalOrdered") \
    .orderBy("TotalOrdered", ascending=False) \
    .display()


# COMMAND ----------

df_joined.groupBy("CustomerKey", "FirstName", "LastName") \
    .sum("OrderQuantity") \
    .withColumnRenamed("sum(OrderQuantity)", "TotalQty") \
    .orderBy("TotalQty", ascending=False) \
    .limit(10) \
    .display()


# COMMAND ----------

from pyspark.sql.functions import year, current_date, when, col

# Calculate Age
df_age = df_joined.withColumn("Age", year(current_date()) - year("BirthDate"))

# Define more granular Age Groups
df_age = df_age.withColumn("AgeGroup", when(col("Age") < 25, "<25")
    .when((col("Age") >= 25) & (col("Age") < 35), "25–34")
    .when((col("Age") >= 35) & (col("Age") < 45), "35–44")
    .when((col("Age") >= 45) & (col("Age") < 55), "45–54")
    .when((col("Age") >= 55) & (col("Age") < 65), "55–64")
    .otherwise("65+"))

# Group by AgeGroup and count
df_age.groupBy("AgeGroup").count().orderBy("AgeGroup").display()



# COMMAND ----------

df_joined.groupBy("MaritalStatus").count().display()

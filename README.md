#  AdventureWorks Data Engineering Pipeline By Harshitha Reddy



##  Overview

The project aims to build an end-to-end data pipeline that ingests, transforms, and loads data from the AdventureWorks dataset using Azure services such as Azure Data Factory, Azure Data Lake Storage, Azure Synapse Analytics, and Power BI for reporting.


## Project Goals

1. **Data Ingestion** - Ingest data from the AdventureWorks SQL database stored in Github using Azure Data Factory. Set up pipelines to extract data from multiple source tables such as Customer, Product, Sales etc.
2. **ETL System** - Use **Azure Databricks** to clean and transform the raw AdventureWorks data.
3. **Medallion Architechture** - Implement a **Bronze-Silver-Gold data model** in Azure Data Lake Storage Gen2, where raw AdventureWorks data is ingested (Bronze), cleaned and joined (Silver), and aggregated for reporting (Gold).
4. **Cloud** - The project runs entirely on Microsoft Azure, using services like **Azure Data Factory, Data Lake Storage, and Azure Synapse Analytics** to handle the scale and complexity of enterprise data.
5. **Reporting** - Create interactive dashboards in **Power BI** connected to the curated (gold) layer. Visualize key business metrics such as total revenue, product performance, and customer trends using data from the AdventureWorks dataset.


## Services Used

1. **Azure Data Lake Storage Gen2**  - Azure Data Lake Storage Gen2 is an object storage service that provides manufacturing scalability, data availability, security, and performance. Hierarchical namespace has been enabled for data organization and file-level operations.
2. **Azure IAM** - This is an identity and access management tool which enables us to manage access to Azure services and resources securely.
3. **Azure Data Factory** - A cloud-based data integration service that allows you to create, schedule, and orchestrate ETL workflows to move and transform data at scale.
4. **Azure Databricks** - An Apache Spark–based analytics platform optimized for Azure, used for big data processing, machine learning, and real-time analytics.
5. **Azure Synapse Analytics** - A unified analytics service that combines big data and data warehousing capabilities to query and analyze data at scale using T-SQL or Spark.
6. **Microsoft Power BI** - A business intelligence tool that enables you to visualize, explore, and share data through interactive and real-time dashboards and reports.
   


## Dataset Used

1. AdventureWorks is a Microsoft-provided sample SQL Server database that simulates the data of a fictional bicycle manufacturing company.
2. It contains multiple interrelated tables covering business domains like Sales, products, product subcategories.
3. **Data Source Link** : https://github.com/harshitha0912ml/AdventureWorks_DE_Pipeline/tree/main/Adventureworks_raw_Data


##  Architecture Diagram

![adventureworksarch](https://github.com/user-attachments/assets/23e5d295-788f-4e15-a001-5df4a838ba5d)

## Process Flow
1. **Data Extraction from GitHub → Azure Data Factory** - The raw AdventureWorks CSV files are hosted on GitHub. A pipeline in Azure Data Factory (ADF) is configured to extract these files using a HTTP connector. Data is fetched as-is (raw CSV format) from GitHub and staged for ingestion.
2. **Load to Bronze Layer (Raw Storage)** - The raw data from ADF is loaded into the Bronze layer of Azure Data Lake Storage Gen2 (ADLS Gen2). This layer acts as a raw data repository, preserving original records for audit and traceability.
3. **Transformation Using Azure Databricks (Bronze → Silver)**
A Spark-based ETL pipeline is executed in Azure Databricks. The raw data is:
- Cleaned and normalized
- Converted to Parquet format for optimized storage
4. Transformed data is then written to the Silver layer.
5. **Data Aggregation in Azure Synapse Analytics** (Silver → Gold)
Azure Synapse Analytics connects to the Silver layer using Linked Services and External Tables. Using T-SQL scripts, it performs:
- Business-level aggregations (e.g., total revenue, top-selling products)
- Output tables are saved in the Gold layer for consumption.
6. The Gold layer stores business-ready data in Parquet format, optimized for reporting and analytics.
7. Power BI connects to the Gold layer either: via Azure Synapse Serverless SQL endpoint.
  Dashboards visualize key metrics such as Sales Trends, Customer Segments, and Product Performance.

  ## Key Data Visualizations (Power BI) -

![Average Income By Occupation](https://github.com/user-attachments/assets/84e733eb-4d56-4b9e-8b78-6560460b7d34)  
*Figure 1: Average Income by Occupation*

![visualization (9)](https://github.com/user-attachments/assets/88a48c12-dfad-424c-af09-1940222b6bf6)
*Figure 2: Total Orders per Month*

![visualization (2)](https://github.com/user-attachments/assets/e90a860f-1823-4e64-a6c5-033e0434a276)
*Figure 3: Order Composition by maritial status*

![visualization (7)](https://github.com/user-attachments/assets/dc92c8a4-2085-4b16-a347-e434c5d00ae7)
*Figure 4: Product Composition by Product Type*
















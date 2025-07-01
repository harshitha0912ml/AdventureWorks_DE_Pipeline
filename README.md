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











#  AdventureWorks Data Engineering Pipeline By Harshitha Reddy



##  Overview

The project aims to build an end-to-end data pipeline that ingests, transforms, and loads data from the AdventureWorks dataset using Azure services such as Azure Data Factory, Azure Data Lake Storage, Azure Synapse Analytics, and Power BI for reporting.


## Project Goals

1. **Data Ingestion** - Ingest data from the AdventureWorks SQL database stored in Github using Azure Data Factory. Set up pipelines to extract data from multiple source tables such as Customer, Product, Sales etc.
2. **ETL System** - Use Azure Databricks to clean and transform the raw AdventureWorks data.
3. **Medallion Architechture** - Implement a **Bronze-Silver-Gold data model** in Azure Data Lake Storage Gen2, where raw AdventureWorks data is ingested (Bronze), cleaned and joined (Silver), and aggregated for reporting (Gold).
4. **Cloud** - The project runs entirely on Microsoft Azure, using services like **Azure Data Factory, Data Lake Storage, and Azure Synapse Analytics** to handle the scale and complexity of enterprise data.
5. **Reporting** - Create interactive dashboards in Power BI connected to the curated (gold) layer. Visualize key business metrics such as total revenue, product performance, and customer trends using data from the AdventureWorks dataset.


## Services Used



## Dataset Used



##  Architecture Diagram












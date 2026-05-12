readme_content = """# 🎬 Azure Databricks Medallion Architecture: Movie Data Pipeline

## 📌 Project Overview
This repository contains the core Data Engineering pipelines for an automated ETL process designed to ingest, clean, and structure movie industry data. The project implements a **Medallion Architecture (Bronze -> Silver -> Gold)** using Azure Databricks and PySpark, ensuring data quality and idempotency for downstream analytics.

## 🚀 Business Value
- **Automated Idempotency:** Implemented sophisticated logic to ensure incremental loads never duplicate data, transitioning from partition overwrites to atomic **Delta MERGE** operations.
- **Data Governance:** Standardized data types and enforced strict schemas on read to prevent corrupt files from breaking production pipelines.
- **Optimized Storage:** Transformed raw CSV files into partitioned **Delta Lake** tables, leveraging features like Deletion Vectors and ZSTD compression for high-performance querying.

## 🏗️ Architecture & Tech Stack
- **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
- **Compute & Transformation:** Azure Databricks, PySpark
- **Table Format:** Delta Lake (Managed by Unity Catalog)
- **Orchestration:** Conceptual design for Azure Data Factory (ADF)

## 🛠️ How to Run
To replicate this pipeline in your Databricks environment:
1. **Infrastructure:** Ensure you have access to an ADLS Gen2 account and a Databricks Workspace.
2. **Configuration:** Update your storage paths and credentials in `src/utils/configuration.py`.
3. **Utility Functions:** Ensure `src/utils/common_functions.py` is available, as it is called via `%run` in the ingestion scripts.
4. **Execution Order:** - First, run the dimensional loads: `02.ingestion_file_language.py`.
   - Then, run the transactional incremental loads: `01.ingestion_file_movie.py`.
   - Finally, generate business views in the Gold layer: `01.results_movie_genre_language.py` and `02.results_country_prod_company.py`.
5. **Parameters:** For incremental notebooks, ensure you provide the `p_file_date` and `p_environment` parameters via Databricks Widgets.

## 📁 Repository Structure
```text
├── docs/
│   ├── Entity_Relationship_Diagram.png       # Database schema
│   └── ADF_Pipeline_Architecture.png         # Orchestration flow
├── src/
│   ├── utils/
│   │   ├── configuration.py                  # ADLS Gen2 path variables
│   │   └── common_functions.py               # Reusable PySpark functions
│   ├── pipelines/
│   │   ├── 01.ingestion_file_movie.py        # Incremental load script
│   │   └── 02.ingestion_file_language.py     # Full load script
│   └── analysis/
│       ├── 01.results_movie_genre_language.py # Gold layer (Genre/Language)
│       └── 02.results_country_prod_company.py # Gold layer (Country/Production)
└── README.md                                 # Project documentation

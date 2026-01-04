# School Enrollment & Education Performance Analytics Platform

## 📌 Project Overview
This project implements an **end-to-end Education Analytics Platform** designed to analyze school enrollment and performance data.  
The system follows a **modern ETL architecture** using **Azure Databricks (Serverless)** for data processing, **Airflow** for orchestration, and **Power BI** for visualization.

The platform enables stakeholders to monitor enrollment trends, evaluate school performance, compare targets vs actuals, and support data-driven decision-making.


## 🎯 Objectives
- Build a scalable ETL pipeline for education enrollment data
- Clean and standardize raw datasets
- Generate analytics-ready **Gold tables**
- Automate workflows using **Airflow**
- Visualize insights using **Power BI dashboards**


## 🛠️ Technology Stack
- **Python**
- **Pandas** (analytics & validation)
- **PySpark**
- **Azure Databricks (Serverless Compute)**
- **Delta Lake**
- **Apache Airflow**
- **Power BI**
- **GitHub**


## 🏗️ Architecture Overview
The project follows a **Bronze–Silver–Gold ETL architecture**:

### 🔹 Bronze Layer (Raw Data)
- Ingests multiple raw enrollment CSV files
- Stores combined raw data as Delta tables

### 🔹 Silver Layer (Cleaned Data)
- Handles missing values and inconsistencies
- Standardizes schema and data types
- Produces cleaned Delta tables

### 🔹 Gold Layer (Analytics)
- Aggregated and business-ready datasets
- Optimized for reporting and dashboards
- Used directly by Power BI


## 🔄 ETL Workflow
### 1️⃣ Extract
- Read enrollment datasets from Databricks storage
- Load raw data into Bronze Delta tables

### 2️⃣ Transform
- Data cleaning and validation
- Feature engineering and metric calculations
- Composite metrics such as **school_score**

### 3️⃣ Load
- Write transformed data into Silver and Gold Delta tables
- Make analytics tables available for visualization

📌 **Note:**  
ETL scripts are implemented as **Python-based Databricks notebooks**, which is a standard practice for Spark ETL pipelines.


## 📊 Analytics & KPIs
The Gold layer includes analytics such as:
- Year-wise enrollment trends
- Gender-wise and grade-wise enrollment distribution
- District and school-level performance comparison
- Pass rate and attendance analysis
- Learning growth and skill index metrics
- Composite school performance score
- Ranking of schools (overall and district-wise)
- Target vs actual enrollment analysis (500M annual target)

## ⏱️ Workflow Orchestration - Airflow
An Apache Airflow DAG orchestrates the pipeline with:
  Weekly scheduling
  Retry logic and fault tolerance
  Sequential execution:
    Data Ingestion (Bronze)
    Data Cleaning (Silver)
    Analytics & Gold table generation
    Power BI refresh (simulated)

Airflow triggers **Azure Databricks Serverless Jobs** using a secure **Personal Access Token (PAT)**.


## 📈 Power BI Dashboards
Power BI dashboards are built using Gold tables and include:
KPI visual showing **Target vs Achieved Enrollment**
Enrollment trends over time
Gender and grade distribution
District-wise performance insights
School ranking visuals


## 📁 Repository Structure
'''text
Capstone Project/
├── Airflow/
│   ├── capstone.py
│   ├── Enrollment_Education_Analytics_Pipeline-graph.png
│   ├── school_enrollment_education_pipeline-graph.png
│   └── Integration_of_DataBricks_with_Airflow.jpg
│
├── Notebooks/
│   ├── Data Ingestion.ipynb
│   ├── Data Cleaning Transformation.ipynb
│   └── Data Analytics.ipynb
│
├── Datasets/
│   └── raw_enrollment_files.csv
│
├── Power BI/
│   └── enrollment_dashboard.pbix
│
├── Presentation/
│   └── Final_Project_Presentation.pptx
│
├── README.md
└── .gitignore
'''
        


## 🚀 How to Run the Project
1. Upload raw datasets to Databricks storage
2. Execute Databricks notebooks in order:
   - Ingestion → Cleaning → Analytics
3. Create Databricks Jobs using **Serverless Compute**
4. Update Job IDs in the Airflow DAG
5. Trigger the Airflow DAG (weekly or manual)
6. Connect Power BI to Gold Delta tables



## 🧠 Key Learnings
- Building scalable ETL pipelines using Spark and Delta Lake
- Automating workflows with Airflow
- Designing analytics-ready data models
- Creating business-focused dashboards in Power BI
- Working with Azure Databricks Serverless architecture



## ✅ Project Status
✔ ETL pipeline implemented  
✔ Analytics and Gold tables completed  
✔ Airflow orchestration completed  
✔ Power BI dashboards created  
✔ Documentation completed  


## 📌 Note
This project is developed as part of an academic capstone and demonstrates real-world data engineering and analytics practice

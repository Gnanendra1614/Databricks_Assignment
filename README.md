# Databricks Assignment

## Overview

This repository contains Databricks and PySpark implementations for two data engineering assignments. It covers data ingestion, transformation, API integration, custom schemas, and Delta Lake storage.

## Project Structure

```text
Databricks_Assignment/
│
├── src/
│   ├── question_1/
│   │   ├── 01_bronze_to_silver/
│   │   │   └── employee_bronze_to_silver.py
│   │   │
│   │   ├── 02_silver_to_gold/
│   │   │   └── employee_silver_to_gold.py
│   │   │
│   │   └── 03_source_to_bronze/
│   │       ├── employee_source_to_bronze.py
│   │       └── utils.py
│   │
│   └── question_2/
│       └── api_source_to_silver.py
│
└── README.md
```

## Technologies Used

- Python
- Apache Spark
- PySpark
- Databricks
- Delta Lake
- REST API integration
- Git and GitHub

## Question 1: Employee Data Pipeline

### Objective

Build a data pipeline that ingests Employee, Department, and Country CSV datasets, transforms the data through the Bronze, Silver, and Gold layers, and stores the results in Delta format.

### 1. Source-to-Bronze

**File:** `employee_source_to_bronze.py`

Tasks performed:

- Read Employee, Department, and Country CSV files.
- Create Spark DataFrames using CSV headers and inferred schemas.
- Write the three datasets to the Source-to-Bronze output locations in CSV format.

### 2. Utility Functions

**File:** `utils.py`

Reusable functions include:

- Convert CamelCase column names to snake_case.
- Read CSV data using a custom schema.
- Add a `load_date` column.
- Write DataFrames in CSV format.
- Write DataFrames in Delta format.

### 3. Bronze-to-Silver

**File:** `employee_bronze_to_silver.py`

Tasks performed:

- Read Bronze CSV datasets using custom schemas.
- Standardize column names.
- Join employee records with department and country lookup datasets.
- Include department names and country names.
- Add the `load_date` column.
- Check for duplicate employee IDs.
- Save the cleaned data in Delta format.

**Silver output location:**

```text
/Volumes/dev_catalog/bronze/stream_data/assignment_q1/silver/Employee_info/dim_employee
```

### 4. Silver-to-Gold

**File:** `employee_silver_to_gold.py`

Tasks performed:

- Read Silver Delta data.
- Calculate total salary by department in descending order.
- Calculate employee counts by department and country.
- List department names with their corresponding country names.
- Calculate average employee age by department.
- Add the `at_load_date` column.
- Write the Gold results in Delta format using overwrite mode and a `replaceWhere` condition.

**Gold output location:**

```text
/Volumes/dev_catalog/bronze/stream_data/assignment_q1/gold/employee/fact_employee
```

## Question 2: API Source-to-Silver Pipeline

**File:** `api_source_to_silver.py`

### Objective

Fetch user data from the ReqRes API, process the response using PySpark, derive additional columns, and save the final dataset in Delta format.


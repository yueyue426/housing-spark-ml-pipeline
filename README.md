# Housing ETL ML Pipeline ![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
My goal of this project is to practice building an end-to-end ETL and machine learning pipeline for a regression problem using Apache Spark. The pipeline downloads raw data from Kaggle in CSV format, perform large-scale data cleaning and feature engineering, and transforms the data into batch-partitioned Parquet files stored in cloud storage. The processed data is then used to train and evaluate a regression model, which is saved for future production use. This project demonstrates skills in data engineering fundamentals, distributed data processing, and production-style ML workflows.

## Table of Contents
- [Technologies & Tools](#technologies--tools)
- [About Data](#about-data)
- [Pipeline Diagram](#pipeline-diagram)
- [Project Reproduction](#project-reproduction-try-it-yourself)
- [Project Structure](#project-structure)
- [Future Improvements](#future-improvements)

## Technologies & Tools
#### Google Cloud Platform
- **Google Cloud Platform** (GCP)
- **Google Cloud Storage** (GCS) - Staging data lake, batch-partitioned cleaned datasets and trained ML models
#### Orchestration & Transformation
- **Apache Airflow** - Pipeline orchestration
- **Apache Spark (PySpark)** - For data transformation and feature engineering
#### Machine Leaning
- **Spark MLlib** - For Linear Regression model training and evaluation
#### Enviroment & Language
- **Docker & Docker Compose** - For local development and environments management
- **Python** - Programming Language for data extraction, transformation, and loading

## About Data
The dataset used in this project is downloaded from Kaggle: [California housing price](https://www.kaggle.com/datasets/marcogtt/california-housing). It is derived from the 1990 U.S. Census and contains one row per census block group. The dataset includes 9 features and 1 target variable (median_house_value):

**Features**:
- **longitude** - Geographic coordinate representing the east–west position of the block
- **latitude** - Geographic coordinate representing the north–south position of the block
- **housing_median_age** - Median age of houses in the block
- **total_rooms** - Total number of rooms within the block
- **total_bedrooms** - Total number of bedrooms within the block
- **population** - Total number of people residing within the block
- **households** - Total number of households within the block
- **median_income** - Median household income in the block (measured in tens of thousands of U.S. dollars)
- **ocean_proximity** - Categorical feature indicating proximity to the ocean

**Target Variable**:
- **median_house_value** - Median house value for households in the block (measured in U.S. dollars)

## Pipeline Diagram

## Project Reproduction (Try it Yourself)

## Project Structure
```
.
├── dags/                              # Airflow DAG definitions
│   └── housing_etl_pipeline.py
├── data/raw
│   ├── housing.csv
├── spark_jobs/                        # PySpark scripts for transformation and ML pipeline
│   ├── staging_to_raw_batches.py
│   ├── data_clean.py
│   ├── add_features.py
│   └── ml_model.py
├── docker-compose.yaml                # Defines Airflow services
├── Dockerfile                         # Custom container image
├── requirements.txt                   # Dependencies
├── README.md                          # Project documentation
```
## Sample Model Metrics
- **MAE**: Measures average prediction error in dollar value
- **RMSE**: Penalizes large prediction errors
- **$R^2$**: Indicates variance explained by the model

## Future Improvements

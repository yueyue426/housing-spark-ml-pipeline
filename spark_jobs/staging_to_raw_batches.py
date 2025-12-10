import os
from pyspark.sql import SparkSession
from data_clean import data_clean
from add_features import features_add

def main():
    bucket = os.environ.get("GCS_BUCKET")
    if not bucket:
        raise ValueError("GCS_BUCKET is not set")
    
    input_path = f"gs://{bucket}/staging/housing.csv"
    output_path = f"gs://{bucket}/processed/housing_batches/"

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("staging_to_raw_batches") \
        .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.9-shaded.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/secrets/gcp_service_account.json") \
        .getOrCreate()
    
    # Read .csv file from GCS staging
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df_cleaned = data_clean(df)
    df_featured = features_add(df_cleaned)

    # Partition to 10 batches and write to GCS processed
    df_featured.repartition(10) \
               .write \
               .mode("overwrite") \
               .format("parquet") \
               .save(output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()
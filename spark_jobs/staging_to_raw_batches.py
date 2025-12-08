import os
from pyspark.sql import SparkSession
from data_clean import data_clean
from add_features import features_add

def main():
    bucket = os.environ.get("GCS_BUCKET")
    if not bucket:
        raise ValueError("GCS_BUCKET is not set")
    
    input_path = f"gs://{bucket}/staging/housing.csv"
    output_path = f"gs://{bucket}/raw/housing_batches/"

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("staging_to_raw_batches") \
        .getOrCreate()
    
    # Read .csv file from GCS staging
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df_cleaned = data_clean(df)
    df_featured = features_add(df_cleaned)

    # Partition to 10 batches and write to GCS Raw
    df_featured.repartition(10) \
               .write \
               .mode("overwrite") \
               .format("parquet") \
               .save(output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()
import os
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Constents configuration
GCS_BUCKET = os.environ.get("GCS_BUCKET", "housing_ml_bucket")
TARGET_COL = "median_house_value"
NUM_FEATURES = [
    "longitude",
    "latitude",
    "housing_median_age",
    "total_rooms",
    "total_bedrooms",
    "population",
    "households",
    "median_income",
]
CAT_FEATURE = "ocean_proximity"
INDEXED_FEATURES = "ocean_proximity_idx"
OHE_FEATURES = "ocean_proximity_ohe"
FEATURE_VECTOR_COL = "features"
SCALED_FEATURE_COL = "scaled_features"
TRAIN_RATIO = 0.8

def main():
    input_path = f"gs://{GCS_BUCKET}/processed/housing_batches/"
    output_path = f"gs://{GCS_BUCKET}/models/housing_lr_model/"

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("housing_ml_lr") \
        .getOrCreate()
    
    # Load cleaned data from staging
    df = spark.read.parquet(input_path)

    # Print number of rows and schema
    rowcount = df.count()
    print(f"Total rows: {rowcount}")
    
    print("Schema:")
    df.printSchema()

    # Using StringIndexer convert the strings in column "ocean_proximity"
    indexer = StringIndexer(inputCol=CAT_FEATURE, outputCol=INDEXED_FEATURES, handleInvalid="keep")

    # Using OneHotEncoder to convert indexed column into a binary vector
    encoder = OneHotEncoder(inputCol=INDEXED_FEATURES, outputCol=OHE_FEATURES)

    # Using VectorAssembler to assemble columns
    assembler = VectorAssembler(inputCols=NUM_FEATURES + [OHE_FEATURES], outputCol=FEATURE_VECTOR_COL, handleInvalid="keep")

    # Using StandardScaler to scale features
    scaler = StandardScaler(inputCol=FEATURE_VECTOR_COL, outputCol=SCALED_FEATURE_COL, withStd=True, withMean=False)

    # Create a LinearRegression stage to predict "median_house_value"
    lr = LinearRegression(featuresCol=SCALED_FEATURE_COL, labelCol=TARGET_COL)

    # Built a pipeline
    pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, lr])

    # Split data into training and testing sets using seed 42
    (training_data, testing_data) = df.randomSplit([TRAIN_RATIO, 1-TRAIN_RATIO], seed=42)

    # Fit the pipeline using "training_data"
    train_model = pipeline.fit(training_data)

    # Model evaluation
    predictions = train_model.transform(testing_data)
    evaluator_mae = RegressionEvaluator(predictionCol="prediction", labelCol=TARGET_COL, metricName="mae")
    evaluator_mse = RegressionEvaluator(predictionCol="prediction", labelCol=TARGET_COL, metricName="mse")
    evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol=TARGET_COL, metricName="r2")

    mae = evaluator_mae.evaluate(predictions)
    mse = evaluator_mse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print("=== Evaluation Metrics ===")
    print(f"MAE: {mae:.2f}")
    print(f"MSE: {mse:.2f}")
    print(f"R2: {r2:.2f}")

    # Save model
    train_model.write().overwrite().save(output_path)

    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
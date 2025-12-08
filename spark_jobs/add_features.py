from pyspark.sql.functions import col

# Function definition: define adding features function
def features_add(df):
    # Add feature columns
    df = df.withColumn("rooms_per_household", col("total_rooms") / col("households")) \
           .withColumn("bedrooms_per_room", col("total_bedrooms") / col("total_rooms")) \
           .withColumn("population_per_household", col("population") / col("households"))
    return df
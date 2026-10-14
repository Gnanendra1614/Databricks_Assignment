
from pyspark.sql import functions as F

# Paths
BASE_PATH = "/Volumes/dev_catalog/bronze/stream_data/assignment_q1"

SOURCE_PATH = "/Volumes/dev_catalog/bronze/stream_data"

BRONZE_PATH = f"{BASE_PATH}/source_to_bronze"

# Read Employee CSV
employee_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/Employee-Q1.csv")
)

# Read Department CSV
department_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/Department-Q1.csv")
)

# Read Country CSV
country_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/Country-Q1.csv")
)

# Display source data
print("Employee count:", employee_df.count())
display(employee_df)

print("Department count:", department_df.count())
display(department_df)

print("Country count:", country_df.count())
display(country_df)

# Write source DataFrames to Bronze as CSV directories
employee_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{BRONZE_PATH}/employee_df")

department_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{BRONZE_PATH}/department_df")

country_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{BRONZE_PATH}/country_df")

print("Source-to-Bronze ingestion completed successfully.")

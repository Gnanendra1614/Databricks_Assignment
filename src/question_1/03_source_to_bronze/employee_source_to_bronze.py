
from pyspark.sql import SparkSession

from utils import read_csv, write_delta


# Initialize Spark session
spark = SparkSession.builder.appName(
    "Employee_Source_To_Bronze"
).getOrCreate()


# Source CSV file paths
SOURCE_PATH = "/Volumes/dev_catalog/bronze/stream_data"

employee_path = f"{SOURCE_PATH}/Employee-Q1.csv"
department_path = f"{SOURCE_PATH}/Department-Q1.csv"
country_path = f"{SOURCE_PATH}/Country-Q1.csv"


# Bronze output paths
BRONZE_PATH = "/Volumes/dev_catalog/bronze/assignment_q1"

employee_bronze_path = f"{BRONZE_PATH}/employee"
department_bronze_path = f"{BRONZE_PATH}/department"
country_bronze_path = f"{BRONZE_PATH}/country"


# Read source CSV files
employee_df = read_csv(spark, employee_path)
department_df = read_csv(spark, department_path)
country_df = read_csv(spark, country_path)


# Display source data for validation
print("Employee source data")
employee_df.show(5, truncate=False)

print("Department source data")
department_df.show(5, truncate=False)

print("Country source data")
country_df.show(5, truncate=False)


# Write data to Bronze layer
write_delta(employee_df, employee_bronze_path)
write_delta(department_df, department_bronze_path)
write_delta(country_df, country_bronze_path)


print("All three CSV files loaded into Bronze successfully.")

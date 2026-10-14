
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName(
    "Employee_Silver_To_Gold"
).getOrCreate()

# Read Silver data
SILVER_PATH = (
    "/Volumes/dev_catalog/bronze/stream_data/assignment_q1/silver/employee"
)

silver_df = spark.read.format("delta").load(SILVER_PATH)

# Create department-level business metrics
gold_df = (
    silver_df
    .groupBy("Department", "DepartmentName")
    .agg(
        F.countDistinct("EmployeeID").alias("EmployeeCount"),
        F.round(F.avg("Salary"), 2).alias("AverageSalary"),
        F.max("Salary").alias("MaximumSalary"),
        F.min("Salary").alias("MinimumSalary")
    )
)

# Write Gold data
GOLD_PATH = (
    "/Volumes/dev_catalog/bronze/stream_data/assignment_q1/gold/employee_summary"
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)

print("Silver to Gold aggregation completed!")
display(gold_df)

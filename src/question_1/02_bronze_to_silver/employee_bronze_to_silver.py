
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName(
    "Employee_Bronze_To_Silver"
).getOrCreate()

# Bronze input location
BRONZE_PATH = "/Volumes/dev_catalog/bronze/stream_data/assignment_q1"

# Read Bronze Delta data
employee_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/employee"
)

department_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/department"
)

country_df = spark.read.format("delta").load(
    f"{BRONZE_PATH}/country"
)

# Clean employee data
employee_clean = (
    employee_df
    .dropDuplicates(["EmployeeID"])
    .filter(F.col("EmployeeID").isNotNull())
    .filter(F.col("EmployeeName").isNotNull())
    .withColumn("EmployeeName", F.trim(F.col("EmployeeName")))
    .filter(F.col("EmployeeName") != "")
)

# Clean department and country lookup data
department_clean = (
    department_df
    .drop("_source_file", "_ingested_at")
    .dropDuplicates(["DepartmentID"])
)

country_clean = (
    country_df
    .drop("_source_file", "_ingested_at")
    .dropDuplicates(["CountryCode"])
)

# Join Employee with Department and Country
silver_df = (
    employee_clean
    .join(
        department_clean,
        employee_clean["Department"] == department_clean["DepartmentID"],
        "left"
    )
    .join(
        country_clean,
        employee_clean["Country"] == country_clean["CountryCode"],
        "left"
    )
    .drop("DepartmentID", "CountryCode")
)

# Write Silver Delta data
SILVER_PATH = f"{BRONZE_PATH}/silver/employee"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)

print("Bronze to Silver transformation completed!")
display(silver_df)

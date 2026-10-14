
from pyspark.sql import functions as F

from utils import add_load_date

# Paths
BASE_PATH = "/Volumes/dev_catalog/bronze/stream_data/assignment_q1"
SILVER_PATH = f"{BASE_PATH}/silver/Employee_info/dim_employee"
GOLD_PATH = f"{BASE_PATH}/gold/employee/fact_employee"

# Read Silver Delta data
silver_df = spark.read.format("delta").load(SILVER_PATH)

print("Silver row count:", silver_df.count())
display(silver_df)

# 1. Salary of each department in descending order
salary_by_department = (
    silver_df
    .groupBy("department_name")
    .agg(F.sum("salary").alias("total_salary"))
    .orderBy(F.col("total_salary").desc())
)

print("Salary by department, descending:")
display(salary_by_department)

# 2. Employee count in each department located in each country
employee_count_by_department_country = (
    silver_df
    .groupBy("department_name", "country_name")
    .agg(F.countDistinct("employee_id").alias("employee_count"))
    .orderBy("department_name", "country_name")
)

print("Employee count by department and country:")
display(employee_count_by_department_country)

# 3. Department names with country names
department_country = (
    silver_df
    .select("department_name", "country_name")
    .distinct()
    .orderBy("department_name", "country_name")
)

print("Department names and country names:")
display(department_country)

# 4. Average age per department
average_age_by_department = (
    silver_df
    .groupBy("department_name")
    .agg(F.round(F.avg("age"), 2).alias("average_age"))
    .orderBy("department_name")
)

print("Average age by department:")
display(average_age_by_department)

# 5. Create the combined Gold fact dataset
gold_df = (
    silver_df
    .groupBy("department_id", "department_name", "country_code", "country_name")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.countDistinct("employee_id").alias("employee_count"),
        F.round(F.avg("age"), 2).alias("average_age")
    )
)

# Add Gold load date
gold_df = add_load_date(gold_df, "at_load_date")

print("Gold row count:", gold_df.count())
gold_df.printSchema()
display(gold_df)

# Replace records for the current load date
current_load_date = gold_df.select("at_load_date").first()["at_load_date"]

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "replaceWhere",
        f"at_load_date = '{current_load_date}'"
    )
    .option("overwriteSchema", "true")
    .save(GOLD_PATH)
)

print("Silver-to-Gold transformation completed successfully.")

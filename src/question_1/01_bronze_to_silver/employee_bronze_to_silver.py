
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType
)

from utils import (
    read_csv,
    convert_columns_to_snake_case,
    add_load_date,
    write_delta
)

# Paths
BASE_PATH = "/Volumes/dev_catalog/bronze/stream_data/assignment_q1"
BRONZE_PATH = f"{BASE_PATH}/source_to_bronze"
SILVER_PATH = f"{BASE_PATH}/silver/Employee_info/dim_employee"

# Custom schemas
employee_schema = StructType([
    StructField("EmployeeID", LongType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", LongType(), True),
    StructField("Age", IntegerType(), True)
])

department_schema = StructType([
    StructField("DepartmentID", StringType(), True),
    StructField("DepartmentName", StringType(), True)
])

country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("CountryName", StringType(), True)
])

# Read Bronze CSV datasets
employee_df = read_csv(
    f"{BRONZE_PATH}/employee_df",
    employee_schema
)

department_df = read_csv(
    f"{BRONZE_PATH}/department_df",
    department_schema
)

country_df = read_csv(
    f"{BRONZE_PATH}/country_df",
    country_schema
)

# Convert column names to snake_case using the utility UDF
employee_df = convert_columns_to_snake_case(employee_df)
department_df = convert_columns_to_snake_case(department_df)
country_df = convert_columns_to_snake_case(country_df)

# Join employee data with department and country lookup data
silver_df = (
    employee_df
    .join(
        department_df,
        employee_df.department == department_df.department_id,
        "left"
    )
    .join(
        country_df,
        employee_df.country == country_df.country_code,
        "left"
    )
    .select(
        employee_df.employee_id,
        employee_df.employee_name,
        employee_df.department.alias("department_id"),
        department_df.department_name,
        employee_df.country.alias("country_code"),
        country_df.country_name,
        employee_df.salary,
        employee_df.age
    )
)

# Add load date
silver_df = add_load_date(silver_df, "load_date")

# Check primary-key uniqueness
duplicate_ids = (
    silver_df.groupBy("employee_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Silver row count:", silver_df.count())
print("Duplicate employee IDs:", duplicate_ids.count())
display(silver_df)

# Write cleaned data as Delta
write_delta(silver_df, SILVER_PATH)

print("Bronze-to-Silver transformation completed successfully.")

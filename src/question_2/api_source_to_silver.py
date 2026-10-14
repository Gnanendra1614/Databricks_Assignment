
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, current_date
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

# 1. Create Spark Session

spark = (
    SparkSession.builder
    .appName("API_Source_to_Silver")
    .getOrCreate()
)

# 2. API Configuration
API_URL = "https://reqres.in/api/users"

all_users = []
page = 1

while True:

    response = requests.get(
        API_URL,
        params={"page": page},
        timeout=30
    )

    print(
        f"Fetching page {page} - "
        f"Status code: {response.status_code}"
    )

    response.raise_for_status()

    response_json = response.json()

    users = response_json.get("data", [])

    # Stop when the API returns no users
    if not users:
        print("No more users available.")
        break

    all_users.extend(users)

    page += 1


print("Total users fetched:", len(all_users))

# 3. Define Custom Schema
user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# 4. Create DataFrame Using Custom Schema
users_df = spark.createDataFrame(
    all_users,
    schema=user_schema
)

print("API DataFrame schema:")
users_df.printSchema()

display(users_df)

# 5. Flatten the DataFrame
df_flat = users_df.select(
    col("id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("avatar")
)

print("Flattened DataFrame:")
display(df_flat)

# 6. Derive site_address from email

df_flat = df_flat.withColumn(
    "site_address",
    split(col("email"), "@").getItem(1)
)

print("DataFrame with site_address:")
display(df_flat)

# 7. Add load_date
df_flat = df_flat.withColumn(
    "load_date",
    current_date()
)

print("Final DataFrame schema:")
df_flat.printSchema()

display(df_flat)
# 8. Write DataFrame as Delta
DELTA_PATH = (
    "/Volumes/dev_catalog/bronze/stream_data/"
    "assignment_q1/question_2/silver/site_info/person_info"
)

(
    df_flat.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DELTA_PATH)
)

print("Data written successfully in Delta format.")
print("Output path:", DELTA_PATH)
# 9. Verify Saved Delta Data
saved_df = (
    spark.read
    .format("delta")
    .load(DELTA_PATH)
)

print("Saved row count:", saved_df.count())

saved_df.printSchema()

display(saved_df)

print("API Source-to-Silver pipeline completed.")

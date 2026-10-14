
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import re


# Convert CamelCase column names to snake_case
def camel_to_snake(name):
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


# UDF used to convert column-name strings
camel_to_snake_udf = udf(camel_to_snake, StringType())


def convert_columns_to_snake_case(df):
    # Column names are metadata, so pass their names to the UDF as rows
    names_df = spark.createDataFrame(
        [(name,) for name in df.columns],
        ["column_name"]
    )

    converted_names = [
        row[0]
        for row in names_df.select(
            camel_to_snake_udf("column_name").alias("name")
        ).collect()
    ]

    return df.toDF(*converted_names)


# Add a date column
def add_load_date(df, column_name="load_date"):
    return df.withColumn(column_name, F.current_date())


# Read CSV with header and a supplied schema
def read_csv(path, schema):
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )


# Write a DataFrame as CSV
def write_csv(df, path):
    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )


# Write a DataFrame as Delta
def write_delta(df, path, mode="overwrite"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )

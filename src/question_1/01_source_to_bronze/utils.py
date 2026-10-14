
from pyspark.sql import DataFrame, SparkSession


def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """Read a CSV file with headers and inferred schema."""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path)
    )


def write_delta(
    df: DataFrame,
    table_path: str,
    mode: str = "overwrite"
) -> None:
    """Write a DataFrame to Delta format."""
    (
        df.write
        .format("delta")
        .mode(mode)
        .save(table_path)
    )

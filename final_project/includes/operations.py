# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamWriter

# COMMAND ----------

def read_stream_raw(spark: SparkSession) -> DataFrame:
    """
    Reads stream from `TWEET_SOURCE_PATH` with schema enforcement.
    """
    raw_data_schema = "date STRING, user STRING, text STRING, sentiment STRING"

    return (
        spark.readStream.format("json")
        .schema(raw_data_schema)
        .option("mergeSchema", "true")
        .load(TWEET_SOURCE_PATH)
    )

# COMMAND ----------

def transform_raw(df: DataFrame) -> DataFrame:
    """
    Transforms `df` to include `source_file` and `processing_time` columns.
    """
    return df.select(
        "date",
        "text",
        "user",
        "sentiment",
        input_file_name().alias("source_file"),
        current_timestamp().alias("processing_time"),
    )

# COMMAND ----------

def create_stream_writer(
    df: DataFrame,
    checkpoint: str,
    queryName: str,
    mode: str = "append",
) -> DataStreamWriter:
    """
    Creates stream writer object with checkpointing at `checkpoint`
    """
    return (
        df.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .queryName(queryName)
    )

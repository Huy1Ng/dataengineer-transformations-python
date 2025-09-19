import logging

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)
    # SQL
    # input_df.createOrReplaceTempView("kafka")
    # out_df = spark.sql(
    #     r"""with explode_text as (select explode(regexp_extract_all(lower(value), r"[\w'']+", 0)) as word from kafka) select word, count(*) as count from explode_text group by word order by word asc"""
    # )
    # Dataframe
    out_df = (
        input_df.withColumn(
            "word",
            F.explode(F.regexp_extract_all(F.lower("value"), F.lit(r"[\w']+"), 0)),
        )
        .groupBy("word")
        .count()
        .sort("word", ascending=True)
    )
    out_df.explain()
    out_df.show()
    logging.info("Writing csv to directory: %s", output_path)

    out_df.write.csv(output_path, header=True)

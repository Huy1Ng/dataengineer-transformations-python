from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def calculate_haversine_distance(
    lat1: F.Column, lon1: F.Column, lat2: F.Column, lon2: F.Column
) -> F.Column:
    earth_radius = 3963  # in miles
    theta_1 = F.radians(lat1)
    theta_2 = F.radians(lat2)
    delta_theta = F.radians(lat2 - lat1)
    delta_lambda = F.radians(lon2 - lon1)

    a = (F.sin(delta_theta / 2) ** 2) + F.cos(theta_1) * F.cos(theta_2) * (
        F.sin(delta_lambda / 2) ** 2
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    return F.round(earth_radius * c, 3)


@F.udf(returnType=DoubleType())
def udf_calculate_haversine_distance(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, atan2, sqrt

    earth_radius = 3963  # in miles
    theta_1 = radians(lat1)
    theta_2 = radians(lat2)
    delta_theta = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)

    a = (sin(delta_theta / 2) ** 2) + cos(theta_1) * cos(theta_2) * (
        sin(delta_lambda / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return round(earth_radius * c, 3)


def compute_distance(
    _spark: SparkSession, dataframe: DataFrame, calc_func
) -> DataFrame:
    out_df = dataframe.withColumn(
        "distance",
        calc_func(
            F.col("start_station_latitude"),
            F.col("start_station_longitude"),
            F.col("end_station_latitude"),
            F.col("end_station_longitude"),
        ),
    )
    return out_df


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(
        spark, input_dataset, calculate_haversine_distance
    )
    dataset_with_distances.explain()
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode="append")

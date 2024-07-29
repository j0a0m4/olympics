# Initialize Spark
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

# Path Variables
root_path = "./data"

path = {
    "csv": root_path + "/csv/",
    "parquet": root_path + "/parquet/",
}


def build_parquet_path(filename: str):
    return path["parquet"] + filename + ".parquet"


def save_parquet(df: DataFrame, filename: str) -> str:
    parquet_path = build_parquet_path(filename)
    df.write.mode("overwrite").parquet(parquet_path)
    return parquet_path


def load_parquet(filename: str) -> DataFrame:
    parquet_path = build_parquet_path(filename)
    return spark.read.parquet(parquet_path)


def load_dataframes(datasets: list[str]) -> dict[str, DataFrame]:
    storage = {}
    for dataset in datasets:
        df = load_parquet(dataset)
        storage[dataset] = df
    return storage

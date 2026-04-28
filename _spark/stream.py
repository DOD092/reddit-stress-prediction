import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from _constants import *
from _spark.predict import predict_stream
from _spark.preprocess import preprocess_df


def find_best_model_path(models_dir: str) -> str:
    if not os.path.exists(models_dir):
        raise FileNotFoundError(f"Không tìm thấy thư mục models: {models_dir}")

    best_model_folders = sorted(
        [
            folder
            for folder in os.listdir(models_dir)
            if folder.startswith("Best_model_")
            and os.path.isdir(os.path.join(models_dir, folder))
        ]
    )

    if not best_model_folders:
        raise FileNotFoundError("Không tìm thấy thư mục Best_model_ trong models")

    return os.path.join(models_dir, best_model_folders[0])


def structured_stream():
    print("START STRUCTURED STREAM")

    spark_session = (
        SparkSession.builder
        .master(SPARK_MASTER_HOST)
        .appName(SPARK_ONLINE_APP_NAME)
        .config("spark.jars.packages", ",".join(SPARK_STREAM_PACKAGE))
        .config("spark.dynamicAllocation.enabled", "true")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("WARN")

    print("READ STREAM FROM KAFKA...")
    stream_df = (
        spark_session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_CRAWL_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_column = col("value").cast("string")

    infer_schema_df = stream_df.select(
        from_json(json_column, "map<string,string>").alias("parsed_data")
    )

    streaming_df = infer_schema_df.selectExpr(
        "parsed_data.subreddit as subreddit",
        "parsed_data.post_id as post_id",
        "parsed_data.text as text",
        "parsed_data.social_timestamp as social_timestamp",
        "parsed_data.url as url",
    )

    print("START PREPROCESS STREAM DATA...")
    preprocessed_df = preprocess_df(streaming_df, TRAIN_SET_PATH)

    best_model_path = find_best_model_path(MODELS_DIR)
    print(f"USING BEST MODEL: {best_model_path}")

    print("START PREDICT STREAM...")
    predicted_df = predict_stream(preprocessed_df, best_model_path)

    print("PREDICTED DATAFRAME SCHEMA:")
    predicted_df.printSchema()

    kafka_df = predicted_df.selectExpr("to_json(struct(*)) as value")

    query = (
    kafka_df.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", KAFKA_DETECTED_TOPIC)
    .option("checkpointLocation", DETECTED_RESULT_CSV_CHECKPOINT_PATH)
    .trigger(processingTime=STREAM_TRIGGER_TIME)
    .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    structured_stream()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("spark://localhost:7077")
    .appName("test_spark_cluster")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "hello"), (2, "spark"), (3, "cluster")],
    ["id", "text"]
)

df.show()

print("Spark Master:", spark.sparkContext.master)
print("Spark UI:", spark.sparkContext.uiWebUrl)

spark.stop()
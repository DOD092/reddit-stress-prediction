from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import Word2VecModel, RegexTokenizer

from _constants import *

def read_csv(input_path):
    spark_session = SparkSession.builder\
        .master(SPARK_MASTER_HOST)\
        .appName(SPARK_OFFLINE_APP_NAME)\
        .config("spark.dynamicAllocation.enabled", "false")\
        .config("spark.cores.max", "2")\
        .config("spark.executor.cores", "1")\
        .config("spark.executor.memory", "1g")\
        .config("spark.driver.memory", "1g")\
        .config("spark.driver.bindAddress", "0.0.0.0")\
        .getOrCreate()

    df_input = spark_session.read.csv(input_path, header=True, inferSchema=True)
    df_input = df_input.withColumn("label", col("label").cast("int"))
    return df_input

def write_csv(df_output, output_path):
    df_output.write.csv(output_path, header=True, mode="overwrite")

def tokenize(data_input_df):
    tokenizer = RegexTokenizer(
        inputCol="words_str",
        outputCol="words",
        pattern="\\W"
    )
    return tokenizer.transform(data_input_df)

def load_model_W2V(w2v_model_path):
    model_w2v = Word2VecModel.load(w2v_model_path)
    return model_w2v
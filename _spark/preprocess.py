import re, string
from pyspark.sql.functions import col, lower, regexp_replace, concat_ws
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.ml import Pipeline

from _spark.utils import read_csv, write_csv

def preprocess_csv(data_input_path, df_fit_path, data_output_path):
    print("START PREPROCESS CSV")
    df=read_csv(data_input_path)
    df_fit=read_csv(df_fit_path)

    df=df.filter((df["label"]==0) | (df["label"]==1))
    df=(
        df\
        .withColumn("text", lower(col("text")))\
        .withColumn("text", regexp_replace(col("text"), r"\[.*?\]", ""))\
        .withColumn("text", regexp_replace(col("text"), r"https?://\S+|www\.\S+", ""))\
        .withColumn("text", regexp_replace(col("text"), r"<.*?>+", ""))\
        .withColumn("text", regexp_replace(col("text"), "[%s]" % re.escape(string.punctuation), ""))\
        .withColumn("text", regexp_replace(col("text"), "\n", " "))\
        .withColumn("text", regexp_replace(col("text"), "\w*\d\w*", ""))
    )
    
    tokenizer=RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    remover=StopWordsRemover(inputCol="words", outputCol="filtered")
    pipeline=Pipeline(stages=[tokenizer, remover])
    df=pipeline.fit(df_fit).transform(df)
    df=df.na.drop()
    df=df.withColumn("words_str", concat_ws(" ", col("words")))
    df=df.withColumn("filtered_str", concat_ws(" ", col("filtered")))
    df=df.drop("words", "filtered")
    write_csv(df, data_output_path)
    df.sparkSession.stop()
    print("END PREPROCESS CSV")

def preprocess_df(df_input, df_fit_path):
    print("START PREPROCESS DF")
    df=df_input
    df_fit=read_csv(df_fit_path)

    df=(
        df\
        .withColumn("text", lower(col("text")))\
        .withColumn("text", regexp_replace(col("text"), r"\[.*?\]", ""))\
        .withColumn("text", regexp_replace(col("text"), r"https?://\S+|www\.\S+", ""))\
        .withColumn("text", regexp_replace(col("text"), r"<.*?>+", ""))\
        .withColumn("text", regexp_replace(col("text"), "[%s]" % re.escape(string.punctuation), ""))\
        .withColumn("text", regexp_replace(col("text"), "\n", " "))\
        .withColumn("text", regexp_replace(col("text"), "\w*\d\w*", ""))
    )
    
    tokenizer=RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    remover=StopWordsRemover(inputCol="words", outputCol="filtered")
    df = tokenizer.transform(df)
    df = remover.transform(df)
    df=df.na.drop()
    df=df.withColumn("words_str", concat_ws(" ", col("words")))
    df=df.withColumn("filtered_str", concat_ws(" ", col("filtered")))
    print("END PREPROCESS DF")
    return df
    
    
from pyspark.ml import Pipeline
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier, DecisionTreeClassifier,LinearSVC

from _spark.utils import read_csv,load_model_W2V,tokenize

def W2V(data_input_path, model_output_path, **kwargs):
    df=read_csv(data_input_path)
    df=tokenize(df)
    sentences=df.select("words")
    word2Vec = Word2Vec(vectorSize=100, minCount=1, inputCol='words', outputCol='wordEmbeddings')
    w2v_model=word2Vec.fit(sentences)

    # save model
    w2v_model_path=model_output_path
    w2v_model.write().overwrite().save(w2v_model_path)
    # Push model_output_path to xcom  
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)
    print(f"Saved W2V model to: {w2v_model_path}")

def SVM(data_input_path, model_w2v_path, model_output_path, **kwargs):
    df=read_csv(data_input_path)
    df=tokenize(df)
    w2v_model=load_model_W2V(model_w2v_path)
    svm=LinearSVC(featuresCol="wordEmbeddings", labelCol="label", predictionCol="label_pred")
    pipeline_svm=Pipeline(stages=[w2v_model, svm])
    model_svm=pipeline_svm.fit(df)

    # save model
    svm_model_path=model_output_path
    model_svm.write().overwrite().save(svm_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)
    print(f"Saved SVM model to: {svm_model_path}")

def RandomForest(data_input_path, model_w2v_path, model_output_path, **kwargs):
    df=read_csv(data_input_path)
    df=tokenize(df)
    w2v_model=load_model_W2V(model_w2v_path)
    rf=RandomForestClassifier(featuresCol="wordEmbeddings", labelCol="label", predictionCol="label_pred")
    pipeline_rf=Pipeline(stages=[w2v_model, rf])
    model_rf=pipeline_rf.fit(df)

    # save model
    rf_model_path=model_output_path
    model_rf.write().overwrite().save(rf_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)
    print(f"Saved Random Forest model to: {rf_model_path}")

def LR(data_input_path, model_w2v_path, model_output_path, **kwargs):
    df=read_csv(data_input_path)
    df=tokenize(df)
    w2v_model=load_model_W2V(model_w2v_path)
    lr=LogisticRegression(featuresCol="wordEmbeddings", labelCol="label", predictionCol="label_pred")
    pipeline_lr=Pipeline(stages=[w2v_model, lr])
    model_lr=pipeline_lr.fit(df)

    # save model
    lr_model_path=model_output_path
    model_lr.write().overwrite().save(lr_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)
    print(f"Saved Logistic Regression model to: {lr_model_path}")

def GradientBoosted(data_input_path, model_w2v_path, model_output_path, **kwargs):
    df=read_csv(data_input_path)
    df=tokenize(df)
    w2v_model=load_model_W2V(model_w2v_path)
    gbt=GBTClassifier(featuresCol="wordEmbeddings", labelCol="label", predictionCol="label_pred")
    pipeline_gbt=Pipeline(stages=[w2v_model, gbt])
    model_gbt=pipeline_gbt.fit(df)

    # save model
    gbt_model_path=model_output_path
    model_gbt.write().overwrite().save(gbt_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)
    print(f"Saved Gradient Boosted Trees model to: {gbt_model_path}")

def DecisionTree(data_input_path, model_w2v_path, model_output_path, **kwargs):
    df=read_csv(data_input_path)
    df=tokenize(df)
    w2v_model=load_model_W2V(model_w2v_path)
    dt=DecisionTreeClassifier(featuresCol="wordEmbeddings", labelCol="label", predictionCol="label_pred")
    pipeline_dt=Pipeline(stages=[w2v_model, dt])
    model_dt=pipeline_dt.fit(df)

    # save model
    dt_model_path=model_output_path
    model_dt.write().overwrite().save(dt_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)
    print(f"Saved Decision Tree model to: {dt_model_path}")
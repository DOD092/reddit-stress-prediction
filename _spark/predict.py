import os
import json
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from _spark.utils import read_csv, tokenize
from _constants import PROJECT_ROOT


def predict_model(data_input_path, model_predict_path):
    print("START PREDICT MODEL")
    test_data = read_csv(data_input_path)
    test_data = tokenize(test_data)

    model = PipelineModel.load(model_predict_path)
    predictions = model.transform(test_data)
    return predictions


def predict_stream(data_input, model_predict_path):
    print("START PREDICT STREAM")
    test_data = data_input
    predict_model = PipelineModel.load(model_predict_path)
    predictions = predict_model.transform(test_data)
    result = predictions.select('label_pred', 'subreddit', 'post_id', 'text', 'social_timestamp')
    return result


def calc_predict_acc(data_input_path, model_predict_path, **kwargs):
    ti = kwargs["ti"]

    predictions = predict_model(data_input_path, model_predict_path)

    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="label_pred",
        metricName="accuracy"
    )
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="label_pred",
        metricName="weightedPrecision"
    )
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="label_pred",
        metricName="weightedRecall"
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="label_pred",
        metricName="f1"
    )

    accuracy = evaluator_acc.evaluate(predictions)
    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)

    percentage_matching = accuracy * 100

    print("-----------------------------------------")
    print("MODEL EVALUATION")
    print(f"Model path: {model_predict_path}")
    print(f"Accuracy  : {accuracy}")
    print(f"Precision : {precision}")
    print(f"Recall    : {recall}")
    print(f"F1 Score  : {f1}")
    print(f"Accuracy% : {percentage_matching}")
    print("-----------------------------------------")

    output_dir = os.path.join(PROJECT_ROOT, "result_models")
    os.makedirs(output_dir, exist_ok=True)

    model_name = os.path.basename(model_predict_path.rstrip("/"))
    output_path = os.path.join(output_dir, f"{model_name}_metrics.json")

    result = {
        "model_name": model_name,
        "model_path": model_predict_path,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "percentage_matching": percentage_matching
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)

    print(f"Saved metrics to: {output_path}")

    ti.xcom_push(key="predict_acc_result", value=result)
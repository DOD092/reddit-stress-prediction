import sys
import os
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator

from _constants import *
from _airflow.utils import save_best_model, clear_xcoms 
from _spark.preprocess import preprocess_csv
from _spark.train_models import W2V, SVM, RandomForest, LR, GradientBoosted, DecisionTree
from _spark.predict import calc_predict_acc


default_args = {
    "owner": "DAT",
    "start_date": datetime(2026, 1, 1),
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="offline_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["reddit_predict", "offline", "spark"],
) as dag:

    preprocessing_train_set = PythonOperator(
        task_id="train_set_preprocess",
        python_callable=preprocess_csv,
        op_kwargs={
            "data_input_path": TRAIN_SET_PATH,
            "df_fit_path": TRAIN_SET_PATH,
            "data_output_path": TRAIN_PREPROCESSED_PATH,
        },
    )

    preprocessing_test_set = PythonOperator(
        task_id="test_set_preprocess",
        python_callable=preprocess_csv,
        op_kwargs={
            "data_input_path": TEST_SET_PATH,
            "df_fit_path": TRAIN_SET_PATH,
            "data_output_path": TEST_PREPROCESSED_PATH,
        },
    )

    training_w2v = PythonOperator(
        task_id="train_model_W2V",
        python_callable=W2V,
        op_kwargs={
            "data_input_path": TRAIN_PREPROCESSED_PATH,
            "model_output_path": W2V_MODEL_PATH,
        },
    )

    training_svm = PythonOperator(
        task_id="train_model_SVM",
        python_callable=SVM,
        op_kwargs={
            "data_input_path": TRAIN_PREPROCESSED_PATH,
            "model_w2v_path": W2V_MODEL_PATH,
            "model_output_path": SVM_MODEL_PATH,
        },
    )

    predicting_svm = PythonOperator(
        task_id="predict_model_SVM",
        python_callable=calc_predict_acc,
        op_kwargs={
            "data_input_path": TEST_PREPROCESSED_PATH,
            "model_predict_path": SVM_MODEL_PATH,
        },
    )

    training_rf = PythonOperator(
        task_id="train_model_RandomForest",
        python_callable=RandomForest,
        op_kwargs={
            "data_input_path": TRAIN_PREPROCESSED_PATH,
            "model_w2v_path": W2V_MODEL_PATH,
            "model_output_path": RANDOM_FOREST_MODEL_PATH,
        },
    )

    predicting_rf = PythonOperator(
        task_id="predict_model_RandomForest",
        python_callable=calc_predict_acc,
        op_kwargs={
            "data_input_path": TEST_PREPROCESSED_PATH,
            "model_predict_path": RANDOM_FOREST_MODEL_PATH,
        },
    )

    training_lr = PythonOperator(
        task_id="train_model_LR",
        python_callable=LR,
        op_kwargs={
            "data_input_path": TRAIN_PREPROCESSED_PATH,
            "model_w2v_path": W2V_MODEL_PATH,
            "model_output_path": LOGISTIC_REGRESSION_MODEL_PATH,
        },
    )

    predicting_lr = PythonOperator(
        task_id="predict_model_LR",
        python_callable=calc_predict_acc,
        op_kwargs={
            "data_input_path": TEST_PREPROCESSED_PATH,
            "model_predict_path": LOGISTIC_REGRESSION_MODEL_PATH,
        },
    )

    training_gb = PythonOperator(
        task_id="train_model_GradientBoosted",
        python_callable=GradientBoosted,
        op_kwargs={
            "data_input_path": TRAIN_PREPROCESSED_PATH,
            "model_w2v_path": W2V_MODEL_PATH,
            "model_output_path": GRADIENT_BOOSTED_MODEL_PATH,
        },
    )

    predicting_gb = PythonOperator(
        task_id="predict_model_GradientBoosted",
        python_callable=calc_predict_acc,
        op_kwargs={
            "data_input_path": TEST_PREPROCESSED_PATH,
            "model_predict_path": GRADIENT_BOOSTED_MODEL_PATH,
        },
    )

    training_dt = PythonOperator(
        task_id="train_model_DecisionTree",
        python_callable=DecisionTree,
        op_kwargs={
            "data_input_path": TRAIN_PREPROCESSED_PATH,
            "model_w2v_path": W2V_MODEL_PATH,
            "model_output_path": DECISION_TREES_MODEL_PATH,
        },
    )

    predicting_dt = PythonOperator(
        task_id="predict_model_DecisionTree",
        python_callable=calc_predict_acc,
        op_kwargs={
            "data_input_path": TEST_PREPROCESSED_PATH,
            "model_predict_path": DECISION_TREES_MODEL_PATH,
        },
    )

    saving_model = PythonOperator(
        task_id="save_best_model",
        python_callable=save_best_model,
    )


    [preprocessing_train_set, preprocessing_test_set] >> training_w2v

    training_w2v >> training_svm >> predicting_svm
    training_w2v >> training_rf >> predicting_rf
    training_w2v >> training_lr >> predicting_lr
    training_w2v >> training_gb >> predicting_gb
    training_w2v >> training_dt >> predicting_dt

    [predicting_svm, predicting_rf, predicting_lr, predicting_gb, predicting_dt] >> saving_model 
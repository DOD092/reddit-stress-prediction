import os
import shutil
from airflow import settings
from airflow.models import XCom
from sqlalchemy import create_engine
from _constants import MODELS_DIR


def save_best_model(**kwargs):
    ti = kwargs['ti']

    xcom_keys = ti.xcom_pull(
        key='predict_acc_result',
        task_ids=[
            'predict_model_SVM',
            'predict_model_RandomForest',
            'predict_model_LR',
            'predict_model_GradientBoosted',
            'predict_model_DecisionTree'
        ]
    )

    try:
        max_acc = -1
        max_acc_model_path = None

        for xcom_key in xcom_keys:
            if not xcom_key:
                continue

            percentage_matching = xcom_key['percentage_matching']
            model_path = xcom_key['model_path']

            if percentage_matching > max_acc:
                max_acc = percentage_matching
                max_acc_model_path = model_path

        if not max_acc_model_path:
            raise ValueError("Không tìm thấy best model từ XCom")

        if not os.path.exists(max_acc_model_path):
            raise FileNotFoundError(f"Model path không tồn tại: {max_acc_model_path}")

        # Delete old best model
        for folder in os.listdir(MODELS_DIR):
            if folder.startswith('Best_model_'):
                shutil.rmtree(os.path.join(MODELS_DIR, folder), ignore_errors=True)

        best_model_name = os.path.basename(max_acc_model_path)
        best_model_path = os.path.join(
            MODELS_DIR,
            f'Best_model_{best_model_name}_{max_acc}'
        )

        shutil.copytree(max_acc_model_path, best_model_path)
        print(f'Saved best model to: {best_model_path}')

    except Exception as e:
        print(f'Failed to save best model: {e}')
        raise


def clear_xcoms():
    try:
        engine = create_engine(settings.SQL_ALCHEMY_CONN)
        with engine.connect() as connection:
            connection.execute(
                XCom.__table__.delete().where(
                    XCom.key.in_(['predict_acc_result', 'model_output_path'])
                )
            )
            connection.commit()

        print("XCOMs deleted successfully.")

    except Exception as e:
        print(f'Failed to delete XCOMs: {e}')
        raise
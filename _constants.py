import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

TEST_SET_PATH = os.path.join(PROJECT_ROOT, 'data', 'dreaddit-test.csv')
TEST_PREPROCESSED_PATH = os.path.join(PROJECT_ROOT, 'data', 'test-preprocessed')

TRAIN_SET_PATH = os.path.join(PROJECT_ROOT, 'data', 'dreaddit-train.csv')
TRAIN_PREPROCESSED_PATH = os.path.join(PROJECT_ROOT, 'data', 'train-preprocessed')

MODELS_DIR = os.path.join(PROJECT_ROOT, 'models')
W2V_MODEL_PATH = os.path.join(MODELS_DIR, 'W2V')
SVM_MODEL_PATH = os.path.join(MODELS_DIR, 'SVM')
RANDOM_FOREST_MODEL_PATH = os.path.join(MODELS_DIR, 'RandomForest')
LOGISTIC_REGRESSION_MODEL_PATH = os.path.join(MODELS_DIR, 'LogisticRegression')
GRADIENT_BOOSTED_MODEL_PATH = os.path.join(MODELS_DIR, 'GradientBoosted')
DECISION_TREES_MODEL_PATH = os.path.join(MODELS_DIR, 'DecisionTree')

BOOTSTRAP_SERVERS = 'localhost:9092'
SCALA_VERSION = '2.12'
SPARK_VERSION = '3.5.1'
KAFKA_VERSION = '3.6.0'
KAFKA_TEST_TOPIC = 'test-set'
KAFKA_CRAWL_TOPIC = 'crawl-set'
KAFKA_DETECTED_TOPIC = 'detected-result'

SPARK_STREAM_PACKAGE = [
    f'org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}',
    f'org.apache.kafka:kafka-clients:{KAFKA_VERSION}'
]

SPARK_MASTER_HOST = 'local[*]'
SPARK_OFFLINE_APP_NAME = 'Offline System - Training & Choose Best Model'
SPARK_ONLINE_APP_NAME = 'Online System - Real-time Stress Detection'

DETECTED_RESULT_CSV_PATH = os.path.join(PROJECT_ROOT, 'data', 'detected-result')
DETECTED_RESULT_CSV_CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, 'checkpoints', 'detected-result')

SUBREDDITS = [
    'domesticviolence',
    'anxiety',
    'stress',
    'almosthomeless',
    'assistance',
    'homeless',
    'ptsd',
    'relationships'
]

REDDIT_CLIENT_ID = ''
REDDIT_CLIENT_SECRET = ''
REDDIT_USER_AGENT = ''
REDDIT_USER_NAME = ''
REDDIT_PASSWORD = ''

CRAWL_LIMIT = 200
CRAWL_TRIGGER_LIMIT = 20
CRAWL_TRIGGER_TIME = 1000
STREAM_TRIGGER_TIME = '1 second'


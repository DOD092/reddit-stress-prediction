import os
import sys
import json
from datetime import datetime

from kafka import KafkaConsumer
import snowflake.connector

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from _constants import *

SNOWFLAKE_USER = ""
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_ACCOUNT = ""
SNOWFLAKE_WAREHOUSE = ""
SNOWFLAKE_DATABASE = ""
SNOWFLAKE_SCHEMA = ""


def get_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def consume_raw():
    print("START CONNECT RAW TO SNOWFLAKE")

    conn = get_conn()
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_CRAWL_TOPIC,  
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='snowflake-raw-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for msg in consumer:
        data = msg.value

        try:
            cursor.execute("""
                INSERT INTO RAW_CRAWLED_POSTS
                (subreddit, post_id, text, social_timestamp, url)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                data.get("subreddit"),
                data.get("post_id"),
                data.get("text"),
                data.get("social_timestamp"),
                data.get("url")
            ))

            conn.commit()

            print(f"[{datetime.now()}] RAW inserted: {data.get('post_id')}")

        except Exception as e:
            print("ERROR RAW:", e)


if __name__ == "__main__":
    consume_raw()
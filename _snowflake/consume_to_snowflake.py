# _snowflake/consume_to_snowflake.py

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


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def consume_detected_to_snowflake():
    conn = None
    cursor = None
    consumer = None

    try:
        print("START CONNECTING TO SNOWFLAKE...")

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        print("CONNECTED TO SNOWFLAKE.")
        print(f"DATABASE: {SNOWFLAKE_DATABASE}")
        print(f"SCHEMA: {SNOWFLAKE_SCHEMA}")
        print(f"KAFKA TOPIC: {KAFKA_DETECTED_TOPIC}")

        consumer = KafkaConsumer(
            KAFKA_DETECTED_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="snowflake-detected-consumer",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        print("START INSERT TO SNOWFLAKE...")

        for msg in consumer:
            data = msg.value

            subreddit = data.get("subreddit")
            post_id = data.get("post_id")
            text = data.get("text")
            social_timestamp = data.get("social_timestamp")
            label_pred = data.get("label_pred", 0)

            try:
                label_pred = float(label_pred)
            except Exception:
                label_pred = 0.0

            try:
                cursor.execute(
                    """
                    INSERT INTO PREDICTED_POSTS
                    (subreddit, post_id, text, social_timestamp, label_pred)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        subreddit,
                        post_id,
                        text,
                        social_timestamp,
                        label_pred,
                    ),
                )

                conn.commit()

                print(
                    f"[{datetime.now()}] Inserted: "
                    f"subreddit={subreddit}, post_id={post_id}, label_pred={label_pred}"
                )

            except Exception as e:
                print(f"ERROR INSERTING POST {post_id}: {type(e).__name__}: {e}")

    except KeyboardInterrupt:
        print("STOPPED BY USER.")

    except Exception as e:
        print(f"ERROR consume_detected_to_snowflake: {type(e).__name__}: {e}")
        raise

    finally:
        if consumer is not None:
            consumer.close()
            print("Kafka consumer closed.")

        if cursor is not None:
            cursor.close()
            print("Snowflake cursor closed.")

        if conn is not None:
            conn.close()
            print("Snowflake connection closed.")


if __name__ == "__main__":
    consume_detected_to_snowflake()
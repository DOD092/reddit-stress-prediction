import os
import sys
import csv
import json
from kafka import KafkaProducer
import praw

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from _constants import *


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )


def produce_csv():
    producer = None
    csv_file_path = TEST_SET_PATH
    inserted_count = 0

    try:
        print("START produce_csv()")
        print(f"CSV FILE PATH: {csv_file_path}")
        print(f"KAFKA TOPIC: {KAFKA_CRAWL_TOPIC}")

        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"Không tìm thấy file CSV: {csv_file_path}")

        producer = get_kafka_producer()

        with open(csv_file_path, "r", newline="", encoding="utf-8") as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for row in csv_reader:
                producer.send(KAFKA_CRAWL_TOPIC, value=row)
                inserted_count += 1
                print(f"INSERTED CSV ROW #{inserted_count}")

        producer.flush()
        print(f"Finished producing CSV messages. Total inserted: {inserted_count}")

    except Exception as e:
        print(f"Error in produce_csv: {type(e).__name__}: {e}")
        raise

    finally:
        if producer is not None:
            producer.close()
            print("Kafka producer closed.")


def produce_praw():
    producer = None
    inserted_count = 0

    try:
        print("START produce_praw()")
        print(f"KAFKA TOPIC: {KAFKA_CRAWL_TOPIC}")
        print(f"SUBREDDITS: {SUBREDDITS}")
        print(f"CRAWL_LIMIT: {CRAWL_LIMIT}")
        print(f"CRAWL_TRIGGER_LIMIT: {CRAWL_TRIGGER_LIMIT}")

        producer = get_kafka_producer()

        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            username=REDDIT_USER_NAME,
            password=REDDIT_PASSWORD,
        )

        for sub in SUBREDDITS:
            if inserted_count >= CRAWL_LIMIT:
                break

            print(f"Subreddit: r/{sub}")
            print("-------------------------------------------")

            subreddit = reddit.subreddit(sub)

            try:
                posts = subreddit.new(limit=CRAWL_TRIGGER_LIMIT)
            except Exception as e:
                print(f"Không thể lấy bài từ subreddit '{sub}': {type(e).__name__}: {e}")
                continue

            for post in posts:
                if inserted_count >= CRAWL_LIMIT:
                    break

                try:
                    post_text = getattr(post, "selftext", None)

                    if not post_text or not post_text.strip():
                        continue

                    post_info = {
                        "subreddit": post.subreddit.display_name,
                        "post_id": post.id,
                        "text": post_text,
                        "social_timestamp": str(post.created_utc),
                        "url": post.url,
                    }

                    producer.send(KAFKA_CRAWL_TOPIC, value=post_info)
                    inserted_count += 1

                    print(
                        f"INSERTED POST #{inserted_count}: "
                        f"subreddit={post_info['subreddit']}, post_id={post_info['post_id']}"
                    )

                except Exception as e:
                    print(f"Lỗi khi xử lý một post ở subreddit '{sub}': {type(e).__name__}: {e}")
                    continue

            print("-------------------------------------------")

        producer.flush()
        print(f"Finished producing Reddit posts. Total inserted: {inserted_count}")

        if inserted_count == 0:
            print("WARNING: Không có post nào được gửi vào Kafka.")

    except Exception as e:
        print(f"Error in produce_praw: {type(e).__name__}: {e}")
        raise

    finally:
        if producer is not None:
            producer.close()
            print("Kafka producer closed.")


if __name__ == "__main__":
    produce_praw()
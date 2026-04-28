import sys
import os
import json
from datetime import datetime

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from _constants import *


def count_words(text):
    if not isinstance(text, str):
        return 0
    return len(text.split())


def consume_kafka_messages():
    consumer = KafkaConsumer(
        KAFKA_DETECTED_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

    subreddit_counts = {}
    df = pd.DataFrame(
        columns=['subreddit', 'total_posts', 'unique_posts', 'unique_stress_count', 'avg_words']
    )

    for message in consumer:
        subreddit = message.value.get('subreddit')
        post_id = message.value.get('post_id')
        text = message.value.get('text', '')
        label_pred = message.value.get('label_pred', 0)

        if not subreddit:
            continue

        num_words = count_words(text)

        if subreddit not in subreddit_counts:
            subreddit_counts[subreddit] = {
                'total_posts': 0,
                'unique_post_ids': set(),
                'unique_total_posts_words': 0,
                'unique_stress_count': 0,
            }

        subreddit_counts[subreddit]['total_posts'] += 1

        if post_id not in subreddit_counts[subreddit]['unique_post_ids']:
            subreddit_counts[subreddit]['unique_post_ids'].add(post_id)
            subreddit_counts[subreddit]['unique_total_posts_words'] += num_words

            try:
                if float(label_pred) == 1.0:
                    subreddit_counts[subreddit]['unique_stress_count'] += 1
            except:
                pass

        unique_posts = len(subreddit_counts[subreddit]['unique_post_ids'])
        avg_words = (
            subreddit_counts[subreddit]['unique_total_posts_words'] / unique_posts
            if unique_posts > 0 else 0
        )

        df.loc[subreddit] = {
            'subreddit': subreddit,
            'total_posts': subreddit_counts[subreddit]['total_posts'],
            'unique_posts': unique_posts,
            'unique_stress_count': subreddit_counts[subreddit]['unique_stress_count'],
            'avg_words': avg_words,
        }

        last_updated = datetime.fromtimestamp(message.timestamp / 1000)
        time_div.success(f"Last updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")

        display_df = df.sort_values(by="total_posts", ascending=False)

        df_tbl.dataframe(display_df, hide_index=True, use_container_width=True)
        total_posts_chart.bar_chart(display_df, y=['total_posts'], use_container_width=True)
        unique_posts_chart.bar_chart(display_df, y=['unique_posts'], use_container_width=True)
        stress_posts_chart.bar_chart(display_df, y=['unique_stress_count'], use_container_width=True)
        avg_words_chart.bar_chart(display_df, y=['avg_words'], use_container_width=True)


if __name__ == '__main__':
    st.set_page_config(
        page_title='Detected Result Visualization',
        layout='wide',
        initial_sidebar_state='expanded'
    )

    st.title('VISUALIZE: STRESS DETECTED RESULT')

    logo_path = os.path.join(CURRENT_DIR, 'imgs', 'logo.png')
    st.sidebar.image(logo_path, width=250)

    st.sidebar.markdown(
        """
        Reddit Prediction Project - Detected Result Visualization
        """
    )

    time_div = st.empty()

    df_tbl = st.empty()
    st.markdown(
        '<p style="width:100%;text-align:center;margin:0 0 160px 0">General statistics table</p>',
        unsafe_allow_html=True
    )

    total_posts_chart = st.empty()
    st.markdown(
        '<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the total posts per subreddit bar chart</p>',
        unsafe_allow_html=True
    )

    unique_posts_chart = st.empty()
    st.markdown(
        '<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the unique posts per subreddit bar chart</p>',
        unsafe_allow_html=True
    )

    stress_posts_chart = st.empty()
    st.markdown(
        '<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the unique stress posts per subreddit bar chart</p>',
        unsafe_allow_html=True
    )

    avg_words_chart = st.empty()
    st.markdown(
        '<p style="width:100%;text-align:center;margin:0 0 160px 0">Compares the average words of post per subreddit bar chart</p>',
        unsafe_allow_html=True
    )

    consume_kafka_messages()
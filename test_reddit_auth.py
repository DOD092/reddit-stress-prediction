import praw
from _constants import *

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
    username=REDDIT_USER_NAME,
    password=REDDIT_PASSWORD,
)

try:
    me = reddit.user.me()
    print("OK:", me)
except Exception as e:
    print(type(e).__name__, e)
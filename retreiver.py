import argparse
import os
from datetime import datetime
from pprint import pprint

import praw
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description='extract related posts and comments from reddit')
parser.add_argument('--stream', type=bool, default=False, help='enable streaming mode')
parser.add_argument('--subs', type=str, default="gaming+playstation+xbox+pcgaming", help='subreddits to look for')
parser.add_argument('--tags', type=str, default="friend, online, multiplayer", help='keywords to look for')
parser.add_argument('--limit', type=int, default=1000, help='total number of data gathered')

args = parser.parse_args()

reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('SECRET'),
    user_agent=os.getenv('USER_AGENT'),
)
assert reddit.read_only
print('initialized reddit client')

subs = reddit.subreddit(args.subs)

if args.stream:
    for comment in subs.stream.comments(skip_existing=True):
        print("{}".format(comment.body))

else:
    data = []
    for cm in subs.comments(limit=args.limit):
        data.append((len(data), cm.id, datetime.fromtimestamp(cm.created), cm.body))
    print(len(data))

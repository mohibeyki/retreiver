import argparse
import os
import sys
from datetime import datetime
from pprint import pprint

import pandas as pd
import praw
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(
    description='extract related posts and comments from reddit')
parser.add_argument('--subs', type=str,
                    default="pokemon+pokemongo+PokemonGoFriends+PokemonGoStories", help='subreddits to look for')
parser.add_argument('--tags', type=str,
                    default="friend,companion,depression,social,community,raid", help='keywords to look for')
parser.add_argument('--limit', type=int, default=10000,
                    help='total number of data gathered')

args = parser.parse_args()

reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('SECRET'),
    user_agent=os.getenv('USER_AGENT'),
)
assert reddit.read_only
print('initialized reddit client')

subs = reddit.subreddit(args.subs)
tags = args.tags.split(',')

data = []

try:
    for cm in subs.comments(limit=args.limit):
        if any(tag in cm.body for tag in tags):
            body = cm.body.replace('\n', ' ').replace('\r', '')
            print(len(data), cm.id, datetime.fromtimestamp(cm.created), body)
            data.append([cm.id, datetime.fromtimestamp(cm.created), body])

    for cm in subs.stream.comments(skip_existing=True):
        if any(tag in cm.body for tag in tags):
            body = cm.body.replace('\n', ' ').replace('\r', '')
            print(len(data), cm.id, datetime.fromtimestamp(cm.created), body)
            data.append([cm.id, datetime.fromtimestamp(cm.created), body])

except KeyboardInterrupt:
    print('Interrupted')
    try:
        df = pd.DataFrame(data, columns=['id', 'created', 'body'])
        print(df)
        df.to_csv('comments.csv')
        sys.exit(0)
    except SystemExit:
        os._exit(0)

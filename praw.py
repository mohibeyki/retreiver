import argparse
import os
from datetime import datetime
from pprint import pprint

import pandas as pd
import praw
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

parser = argparse.ArgumentParser(
    description='extract related posts and comments from reddit')
parser.add_argument('--subs', type=str,
                    default="pokemongo+PokemonGoFriends+PokemonGoStories",
                    help='subreddits to look for')
parser.add_argument('--tags', type=str,
                    default="friend,companion,depression,social,community,raid", help='keywords to look for')
parser.add_argument('--limit', type=int, default=100000,
                    help='total number of data gathered')
args = parser.parse_args()

reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('SECRET'),
    user_agent=os.getenv('USER_AGENT'),
)
assert reddit.read_only
print('initialized reddit client')

try:
    subs = reddit.subreddit(args.subs)
    tags = args.tags.split(',')

    data = []
    checkpoints = 0
    with tqdm(total=args.limit) as pbar:
        pbar.set_postfix({'checkpoints': checkpoints})

        for submission in subs.new(limit=args.limit):
            submission.comments.replace_more(limit=None)
            for comment in submission.comments:
                body = comment.body.replace('\n', ' ').replace('\r', '')
                data.append(
                    [comment.id, datetime.fromtimestamp(comment.created), body])

            pbar.update(1)
            if pbar.n % 10 == 0:
                df = pd.DataFrame(data, columns=['id', 'created', 'body'])
                checkpoints += 1
                pbar.set_postfix({'checkpoints': checkpoints})
                df.to_csv('comments.csv', index=False, mode='a', header=False)
                data = []

except KeyboardInterrupt:
    print('Interrupted')
finally:
    df = pd.DataFrame(data, columns=['id', 'created', 'body'])
    df.to_csv('comments.csv', index=False, mode='a', header=False)

# try:
#     for cm in subs.stream.comments(skip_existing=True):
#         if any(tag in cm.body for tag in tags):
#             body = cm.body.replace('\n', ' ').replace('\r', '')
#             print(len(data), cm.id, datetime.fromtimestamp(cm.created), body)
#             data.append([cm.id, datetime.fromtimestamp(cm.created), body])
# except KeyboardInterrupt:
#     print('Interrupted')
#     try:
#         df = pd.DataFrame(data, columns=['id', 'created', 'body'])
#         df.to_csv('comments.csv')
#         sys.exit(0)
#     except SystemExit:
#         os._exit(0)

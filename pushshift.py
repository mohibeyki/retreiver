import argparse
import json
import os
import subprocess
import time
from datetime import datetime

import pandas as pd
import requests as requests
from tqdm import tqdm

parser = argparse.ArgumentParser(description='extract related posts and comments from reddit')
parser.add_argument('--subs', type=str, default="pokemongo", help='subreddits to look for')
parser.add_argument('--type', type=str, default="comment", help='type of data (subreddit, comment, submission)')
parser.add_argument('--limit', type=int, default=100, help='total number of data per request')
parser.add_argument('--loops', type=int, default=100, help='total number of data gathered')
parser.add_argument('--resume', type=bool, default=True, help='resume from the last run')
args = parser.parse_args()

url = 'https://api.pushshift.io/reddit/{}/search?limit={}&nest_level=1&sort=desc&subreddit={}&before={}'
print(args)

if not args.resume and os.path.exists('{}.csv'.format(args.type)):
    os.remove('{}.csv'.format(args.type))


def get_start_time():
    if args.resume and os.path.exists('{}.csv'.format(args.type)):
        result = subprocess.run(['tail', '-1', '{}.csv'.format(args.type)], stdout=subprocess.PIPE)
        date_string = result.stdout.decode('UTF-8').split(',')[1]
        start_time = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        return int(start_time.timestamp())

    return int(datetime.utcnow().timestamp())


def get_total_items():
    if args.resume and os.path.exists('{}.csv'.format(args.type)):
        result = subprocess.run(['wc', '-l', '{}.csv'.format(args.type)], stdout=subprocess.PIPE)
        total = result.stdout.decode('UTF-8').split()[0]
        return int(total)

    return 0


def send_request(request_url):
    json_response = requests.get(request_url, headers={'User-Agent': "retriever by /u/mohibeyki"})
    time.sleep(0.5)  # rate-limited to a maximum of 120 r/s

    try:
        json_data = json_response.json()
    except json.decoder.JSONDecodeError:
        print('JSON error')
        time.sleep(0.5)
        return None

    if 'data' not in json_data:
        print('received JSON does not have "data" in it')
        return None

    items = json_data['data']
    if len(items) == 0:
        print('received zero items')
        return None

    return items


def collect_comments():
    previous_epoch = get_start_time()
    total = get_total_items()

    p_bar = tqdm(range(args.loops))
    p_bar.set_postfix({'items': str(total)})
    for _ in p_bar:
        data = []
        request_url = url.format(args.type, args.limit, args.subs, str(previous_epoch))

        items = send_request(request_url)
        if items is None:
            continue

        for comment in items:
            body = comment['body'].replace('\n', ' ').replace('\r', '')
            data.append([comment['id'], datetime.fromtimestamp(comment['created_utc']), body])

        # checkpoint
        df = pd.DataFrame(data)
        df.to_csv('{}.csv'.format(args.type), index=False, mode='a', header=False)

        total += len(items)
        p_bar.set_postfix({'items': str(total)})
        previous_epoch = items[-1]['created_utc'] - 1


if __name__ == '__main__':
    collect_comments()

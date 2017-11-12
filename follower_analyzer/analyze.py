"""Collection of various functions used to analyze data."""
import logging
import os
import pickle
import time

from collections import Counter
from collections import defaultdict

from follower_analyzer.db import STATUS_COLUMNS
from follower_analyzer import db2
from follower_analyzer.db2 import USER_COLUMNS2


OBAMA = 'BarackObama'
TRUMP = 'realDonaldTrump'
BOTH = 'BOTH'


def collect_counters(dbpath: str, max_iterations: int=1000, bucket: int=10, query_limit: int=100000):
    counters = {OBAMA: defaultdict(Counter), BOTH: defaultdict(Counter), TRUMP: defaultdict(Counter)}
    start = time.time()
    statuses = {BOTH: set(), TRUMP: set()}
    conn = db2.get_conn(dbpath)
    cursor = conn.cursor()
    cursor.execute('select * from users')
    index = 0
    while True:
        if index == max_iterations:
            break
        else:
            index += 1
        rows = cursor.fetchmany(query_limit)
        for row in rows:
            following = row['following']
            if following == BOTH:
                names = [OBAMA, TRUMP, BOTH]
            else:
                names = [following]

            if following != OBAMA:
                if row['status'] != '':
                    statuses[following].add(row['status'])
                if row['previous_status'] != '':
                    statuses[following].add(row['previous_status'])

            for name in names:
                for k, v in USER_COLUMNS2.items():
                    if k in ['location', 'following']:
                        # TODO: Some of these need special handling.
                        pass
                    elif v == bool or k in ['utc_offset', 'lang'] or k.endswith('_color') or \
                            k.startswith('withheld_'):
                        counters[name][k][row[k]] += 1
                    elif v == int:
                        if row[k] < 10:
                            counters[name][k][row[k]] += 1
                        else:
                            counters[name][k][(row[k] // bucket) * bucket] += 1
                    elif k == 'created_at':
                        date = time.strftime('%Y%m%d', time.strptime(row[k], '%a %b %d %H:%M:%S +0000 %Y'))
                        counters[name][k][date] += 1
                    elif k in ['status', 'previous_status']:
                        if row[k] != '':
                            counters[name][k][1] += 1
                        else:
                            counters[name][k][0] += 1

                followers = row['followers_count']
                friends = row['friends_count']
                if followers > 0 and len(counters[name]['friends_to_followers']) < 10000:
                    ratio = friends / followers
                    if ratio < 1:
                        counters[name]['friends_to_followers'][round(ratio, 1)] += 1
                    else:
                        counters[name]['friends_to_followers'][round(ratio)] += 1
                if friends > 0 and len(counters[name]['followers_to_friends']) < 10000:
                    ratio = followers / friends
                    if ratio < 1:
                        counters[name]['followers_to_friends'][round(ratio, 1)] += 1
                    else:
                        counters[name]['followers_to_friends'][round(ratio)] += 1

        if len(rows) < query_limit:
            break

    cursor.execute('select * from statuses')
    index = 0
    while True:
        if index == max_iterations:
            break
        else:
            index += 1
        rows = cursor.fetchmany(query_limit)
        for row in rows:
            if row['id_str'] in statuses[BOTH]:
                names = [OBAMA, TRUMP, BOTH]
            elif row['id_str'] in statuses[TRUMP]:
                names = [TRUMP]
            else:
                names = [OBAMA]

            for name in names:
                for k, v in STATUS_COLUMNS.items():
                    if k in ['contributors', 'lang', 'possibly_sensitive', 'scopes', 'withheld_copyright',
                             'withheld_in_countries', 'withheld_scope']:
                        counters[name]['status_{}'.format(k)][row[k]] += 1
                    elif k == 'created_at':
                        date = time.strftime('%Y%m%d', time.strptime(row[k], '%a %b %d %H:%M:%S +0000 %Y'))
                        counters[name]['status_{}'.format(k)][date] += 1

        if len(rows) < query_limit:
            break

    with open(dbpath.replace('.db', '.dat'), 'wb') as pfd:
        pickle.dump(counters, pfd)
    logging.debug('Collecting various counts of users and statuses in %s took %s seconds using %s path.',
                  os.path.basename(dbpath), time.time() - start, 'fast' if 'sravan' in dbpath else 'slow')


def merge_counters(counter_paths: list, merged_counter_path: str):
    counters = {OBAMA: defaultdict(Counter), BOTH: defaultdict(Counter), TRUMP: defaultdict(Counter)}
    for cpath in counter_paths:
        start = time.time()
        with open(cpath, 'rb') as pfd:
            for name, breakdown in pickle.load(pfd).items():
                for column, column_counter in breakdown.items():
                    for k, v in column_counter.items():
                        counters[name][column][k] += v
        logging.debug('Merging various counts of users and statuses in %s took %s seconds using %s path.',
                      os.path.basename(cpath), time.time() - start, 'fast' if 'sravan' in cpath else 'slow')

    with open(merged_counter_path, 'wb') as pfd:
        pickle.dump(counters, pfd)

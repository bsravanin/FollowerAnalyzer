"""Following the convention of awful naming."""

import ast
import logging
import sqlite3
import time

from collections import Counter
from collections import OrderedDict
from collections import defaultdict

from ttp.ttp import Parser

from follower_analyzer import db
from follower_analyzer import db2
from follower_analyzer import urls_index


HASHTAGS_COLUMNS = OrderedDict({
    'id_str': str,  # hashtag, primary key
    'statuses_count': int,
    'users_description_count': int,
})
HASHTAGS_INSERT_STMT = db._insert_stmt('hashtags', HASHTAGS_COLUMNS)

URLS_COLUMNS = OrderedDict({
    'id_str': str,  # url, primary key
    'statuses_count': int,
    'users_description_count': int,
    'users_url_count': int,
})
URLS_INSERT_STMT = db._insert_stmt('urls', URLS_COLUMNS)


def get_conn(db_path: str, read_only: bool = True) -> sqlite3.Connection:
    """Get a connection to the DB in the given path. Create schema if necessary."""
    return db._get_conn(db_path, {'hashtags': HASHTAGS_COLUMNS, 'urls': URLS_COLUMNS}, row_factory=sqlite3.Row,
                        read_only=read_only)


def populate_db3(src_db_path: str, dest_db_path: str, urls_index_db_path: str, query_limit: int=100000):
    """Collect stats about hashtags and URLs found in user profiles and tweets. Also updates urls_index with these
    URLs."""
    start = time.time()
    logging.info('Populating hashtags and URLs found in user profiles and tweets.')
    hashtags = defaultdict(Counter)
    urls = defaultdict(Counter)
    parser = Parser()

    with db2.get_conn(src_db_path) as src_conn:
        cursor = src_conn.cursor()
        logging.info('Finding hashtags and URLs in user profiles.')
        cursor.execute('SELECT description, url FROM users WHERE description != "" OR url != ""')
        while True:
            rows = cursor.fetchmany(query_limit)
            for row in rows:
                if row['description'] != '':
                    result = parser.parse(row['description'])
                    for tag in set(result.tags):
                        hashtags[tag.lower()]['users_description_count'] += 1
                    for url in set(result.urls):
                        urls[url]['users_description_count'] += 1
                if row['url'] != '':
                    urls[row['url']]['users_url_count'] += 1
            if len(rows) < query_limit:
                break

        logging.info('Finding hashtags and URLs in tweets.')
        cursor.execute('SELECT hashtags, text FROM statuses')
        while True:
            rows = cursor.fetchmany(query_limit)
            for row in rows:
                result = parser.parse(row['text'])
                for tag in set(ast.literal_eval(row['hashtags']) + result.tags):
                    hashtags[tag.lower()]['statuses_count'] += 1
                for url in set(result.urls):
                    urls[url]['statuses_count'] += 1
            if len(rows) < query_limit:
                break

    logging.info('Inserting and committing %s hashtag rows and %s url rows.',
                 len(hashtags), len(urls), time.time() - start)
    with get_conn(dest_db_path, read_only=False) as dest_conn:
        dest_conn.execute('pragma synchronous = OFF')
        dest_conn.executemany(HASHTAGS_INSERT_STMT, [[key, value['statuses_count'], value['users_description_count']]
                                                     for key, value in hashtags.items()])
        dest_conn.executemany(URLS_INSERT_STMT,
                              [[key, value['statuses_count'], value['users_description_count'], value['users_url_count']]
                               for key, value in urls.items()])
        dest_conn.commit()
    logging.info('Inserting and committing %s hashtag rows and %s url rows took %s seconds.',
                 len(hashtags), len(urls), time.time() - start)
    urls_index.update_index(urls_index_db_path, list(urls.keys()))

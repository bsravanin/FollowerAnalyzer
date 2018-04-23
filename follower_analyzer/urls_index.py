"""Twitter shortens all URLs by default. This is the translator between those shortened URLs to the full URLs
and their domains."""

import logging
import sqlite3
import time

import requests

from collections import defaultdict
from collections import OrderedDict
from multiprocessing.pool import ThreadPool

from follower_analyzer import db


URLS_INDEX_COLUMNS = OrderedDict({
    'id_str': str,  # short_url, primary key
    'full_url': str,
    'domain': str,
})
URLS_INDEX_INSERT_STMT = db._insert_stmt('urls_index', URLS_INDEX_COLUMNS)


def get_conn(db_path: str, read_only: bool = True) -> sqlite3.Connection:
    """Get a connection to the DB in the given path. Create schema if necessary."""
    return db._get_conn(db_path, {'urls_index': URLS_INDEX_COLUMNS}, row_factory=sqlite3.Row, read_only=read_only)


def normalize_url(url: str) -> str:
    """Strips the URL of prefixes like http://, https://, www., and suffix '/'."""
    if url is None:
        url = 'None'
    return url.replace('http://', '').replace('https://', '').replace('www.', '').rstrip('/')


def get_domain(url: str) -> str:
    return normalize_url(url).split('/')[0]


def unshorten_url(url: str, session: requests.Session=None) -> [str, None]:
    """Unshortens a URL following https://stackoverflow.com/a/28918160 and normalizes it."""
    if session is None:
        session = requests.Session()
    try:
        return normalize_url(session.head(url, allow_redirects=True, timeout=5).url)
    except requests.exceptions.Timeout:
        return 'Timeout'
    except requests.exceptions.TooManyRedirects:
        return 'TooManyRedirects'
    except requests.exceptions.ConnectionError:
        return 'ConnectionError'
    except:
        return None


def unshorten_urls_worker(urls: list) -> list:
    session = requests.Session()
    result = []
    for url in urls:
        full_url = unshorten_url(url, session)
        # Should be in the order id_str, full_url, domain
        result.append((normalize_url(url), full_url, get_domain(full_url)))
    return result


def update_index(db_path: str, urls: list, threads: int=64) -> None:
    """Update urls_index with a list of URLs. If they don't already exist, will add a row of their
    (normalized_url, full_url, domain_of_full_url)"""
    start = time.time()
    logging.info('Updating %s with %s URLs.', db_path, len(urls))
    if len(urls) == 0:
        return

    normalized_urls = defaultdict(list)
    for url in urls:
        normalized_urls[normalize_url(url)].append(url)

    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id_str FROM urls_index WHERE id_str IN ({})'
                       .format(', '.join(["'{}'".format(url) for url in normalized_urls.keys()])))

        for row in cursor.fetchall():
            normalized_urls.pop(row[0])

    if len(normalized_urls) == 0:
        logging.info('No new URLs.')
        return

    logging.info('Unshortening %s URLs.', len(normalized_urls))
    to_be_unshortened = [values[0] for values in normalized_urls.values()]
    if len(to_be_unshortened) < threads:
        threads = len(to_be_unshortened)
    batches = []
    size = len(to_be_unshortened) // threads
    for i in range(threads):
        batches.append(to_be_unshortened[size*i:size*(i+1)])
    url_rows = []
    with ThreadPool(threads) as pool:
        for result in pool.map(unshorten_urls_worker, batches):
            url_rows += result

    logging.info('Saving %s new URLs.', len(url_rows))
    with get_conn(db_path, read_only=False) as conn:
        try:
            conn.executemany(URLS_INDEX_INSERT_STMT, url_rows)
        except sqlite3.IntegrityError:
            conflicts = 0
            for row in url_rows:
                try:
                    conn.execute(URLS_INDEX_INSERT_STMT, row)
                except sqlite3.IntegrityError:
                    conflicts += 1
            logging.warning('%s conflicts.', conflicts)
        conn.commit()
    logging.info('Updating %s with %s URLs took %s seconds.', db_path, len(urls), time.time() - start)

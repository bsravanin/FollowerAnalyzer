"""Layer to read Twitter data from DB."""

import gc
import json
import logging
import os
import pickle
import sqlite3
import time

from collections import defaultdict
from multiprocessing.pool import ThreadPool

from follower_analyzer import db


USER_COLUMNS2 = db.USER_COLUMNS.copy()
USER_COLUMNS2['previous_status'] = str
USER_COLUMNS2['following'] = str
USER_INSERT_STMT = 'INSERT INTO {} ({}) VALUES ({})'.format(
    'users',
    ', '.join(["'{}'".format(key) for key in USER_COLUMNS2]),
    ', '.join(['?'] * len(USER_COLUMNS2)))
USER_UPDATE_STMT_SAME_STATUS = 'UPDATE users SET following = "BOTH" WHERE id_str = ?'
USER_UPDATE_STMT_NEW_STATUS = 'UPDATE users SET following = "BOTH", previous_status = ? WHERE id_str = ?'

STATUS_INSERT_STMT = 'INSERT INTO {} ({}) VALUES ({})'.format(
    'statuses',
    ', '.join(["'{}'".format(key) for key in db.STATUS_COLUMNS]),
    ', '.join(['?'] * len(db.STATUS_COLUMNS)))


def get_conn(db_path: str, read_only: bool = False) -> sqlite3.Connection:
    """Get a connection to the DB in the given path. Create schema if necessary."""
    if not os.path.isfile(db_path):
        logging.warning('Could not find an existing DB at %s. Creating one...', db_path)

    if read_only:
        conn = sqlite3.connect('file:{}?mode=ro'.format(db_path), uri=True)
    else:
        conn = sqlite3.connect(db_path)
        db._create_table(conn, 'users', USER_COLUMNS2)
        db._create_table(conn, 'statuses', db.STATUS_COLUMNS)
        conn.commit()

    conn.row_factory = sqlite3.Row
    return conn


def _executemany(conn: sqlite3.Connection, stmt: str, rows: list):
    try:
        conn.executemany(stmt, rows)
    except:
        for row in rows:
            if stmt.startswith('INSERT INTO '):
                conn.execute(stmt.replace('INSERT INTO ', 'INSERT OR IGNORE INTO '), row)
            else:
                conn.execute(stmt.replace('UPDATE ', 'UPDATE OR IGNORE '), row)


def _parallel_commit(args: dict):
    start = time.time()
    root_dir = args['root_dir']
    year = args['year']
    user_rows = args['user_rows']
    status_rows = args['status_rows']

    with open(os.path.join(root_dir, 'twitter_{}.index'.format(year)), 'rb') as pfd:
        index = pickle.load(pfd)

    new_user_rows = []
    existing_user_rows_with_same_status = []
    existing_user_rows_with_new_status = []
    status_rows_dict = {row['id_str']: row for row in status_rows}
    new_status_rows = []
    for row in user_rows:
        new_status = row['status']
        if row['id_str'] in index:
            existing_status = index[row['id_str']]
            if new_status != '' and new_status != existing_status:
                existing_user_rows_with_new_status.append((new_status, row['id_str']))
                new_status_rows.append(status_rows_dict[new_status])
            else:
                existing_user_rows_with_same_status.append((row['id_str'], ))
        else:
            new_user_rows.append(row)
            if new_status != '':
                new_status_rows.append(status_rows_dict[new_status])

    with get_conn(os.path.join(root_dir, 'twitter_{}.db'.format(year))) as conn:
        conn.execute('pragma synchronous = OFF')
        _executemany(conn, USER_INSERT_STMT, new_user_rows)
        _executemany(conn, USER_UPDATE_STMT_SAME_STATUS, existing_user_rows_with_same_status)
        _executemany(conn, USER_UPDATE_STMT_NEW_STATUS, existing_user_rows_with_new_status)

        if len(new_status_rows) > 0:
            _executemany(conn, STATUS_INSERT_STMT, new_status_rows)

        conn.commit()
    logging.debug('Inserting and committing %s user rows and %s status rows for twitter_%s.db took %s seconds.',
                  len(user_rows), len(status_rows), year, time.time() - start)


def _parallel_commit_worker(pool_args_list: list):
    for args in pool_args_list:
        _parallel_commit(args)


def _commit(write_dirs: list, user_rows_by_years: defaultdict, status_rows_by_years: defaultdict,
            threshold: int):
    start = time.time()
    pool_args_lists = [[]] * len(write_dirs)
    years = []
    for year, user_rows in user_rows_by_years.items():
        status_rows = status_rows_by_years[year]
        if len(user_rows) + len(status_rows) >= threshold:
            years.append(year)
            bucket_id = int(year[-2:]) % len(write_dirs)
            pool_args_lists[bucket_id].append({'year': year, 'user_rows': user_rows, 'status_rows': status_rows,
                                               'root_dir': write_dirs[bucket_id]})

    if len(years) > 0:
        pool = ThreadPool(len(write_dirs))
        pool.map(_parallel_commit_worker, pool_args_lists)
        for year in years:
            user_rows_by_years[year].clear()
            status_rows_by_years[year].clear()
    return time.time() - start


def populate_new_dbs(root_dir: str, username: str, offset: int, write_dirs: list=None):
    global_user_row_count = 0
    query_limit = 100000
    commit_threshold = 10000
    status_rows = {}
    user_rows_by_years = defaultdict(list)
    status_rows_by_years = defaultdict(list)

    if write_dirs is None:
        write_dirs = [root_dir]

    for dbname in os.listdir(root_dir):
        if not dbname.startswith(username):
            continue
        conn = db.get_conn(os.path.join(root_dir, dbname), read_only=True)
        conn.row_factory = sqlite3.Row

        user_cursor = conn.cursor()
        user_cursor.execute("select *, '' as previous_status, '{}' as following from users".format(username))

        status_cursor = conn.cursor()
        status_cursor.execute('select * from statuses')
        statuses_left = True

        while True:
            gc.collect()
            if os.path.exists('exit_populate_new_db'):
                logging.info('Found exit marker. Saving all fetched rows and exiting populate_new_db.')
                _commit(write_dirs, user_rows_by_years, status_rows_by_years, 1)
                break

            if os.path.exists('.tunables.json'):
                with open('.tunables.json') as tfd:
                    tunables = json.load(tfd)
                    logging.info('Using tunables %s.', tunables)
                    query_limit = tunables['query_limit']
                    commit_threshold = tunables['commit_threshold']

            user_rows = user_cursor.fetchmany(query_limit)

            if statuses_left and len(status_rows) < len(user_rows):
                _status_rows = status_cursor.fetchmany(query_limit)
                for row in _status_rows:
                    status_rows[row['id_str']] = row

                if len(_status_rows) < query_limit:
                    statuses_left = False

            for row in user_rows:
                global_user_row_count += 1
                if offset >= global_user_row_count:
                    if row['status'] != '':
                        status_rows.pop(row['status'])
                else:
                    year = time.strftime('%Y%m', time.strptime(row['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
                    user_rows_by_years[year].append(row)
                    if row['status'] != '':
                        status_rows_by_years[year].append(status_rows.pop(row['status']))

            logging.info('Partially inserted %s users and corresponding statuses in %s seconds.',
                         global_user_row_count,
                         _commit(write_dirs, user_rows_by_years, status_rows_by_years, commit_threshold))

            if len(user_rows) < query_limit:
                _commit(write_dirs, user_rows_by_years, status_rows_by_years, 1)
                break

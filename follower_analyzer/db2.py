"""This module is specific to the analysis we were doing. We first started with just one user, storing data about all
followers of one user in realDonaldTrump.db. Along similar lines we created a BarackObama.db. Both these DBs had a
large but unknown quantity of overlap. The sheer size of the DBs also resulted in a well-documented performance
limitation for both reads and writes.

This module was created with an aim of consolidating the data in both these DBs, while also splitting them by YYYYMM
(based on when the follower's account was created). The only difference from db.py is in the users table:
2 additional columns, one to keep track of previous_status (because combining users in two DBs with the same user_id
could result in at most 2 statuses) and another to keep track of whether the user follows only realDonaldTrump,
only BarackObama, or both.
"""

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
USER_INSERT_STMT = db._insert_stmt('users', USER_COLUMNS2)
USER_UPDATE_STMT_SAME_STATUS = 'UPDATE users SET following = "BOTH" WHERE id_str = ?'
USER_UPDATE_STMT_NEW_STATUS = 'UPDATE users SET following = "BOTH", previous_status = ? WHERE id_str = ?'

STATUS_INSERT_STMT = db._insert_stmt('statuses', db.STATUS_COLUMNS)


def get_conn(db_path: str, read_only: bool = True) -> sqlite3.Connection:
    """Get a connection to the DB in the given path. Create schema if necessary."""
    return db._get_conn(db_path, {'users': USER_COLUMNS2, 'statuses': db.STATUS_COLUMNS}, row_factory=sqlite3.Row,
                        read_only=read_only)


def _executemany(conn: sqlite3.Connection, stmt: str, rows: list):
    try:
        conn.executemany(stmt, rows)
    except:
        for row in rows:
            if stmt.startswith('INSERT INTO '):
                conn.execute(stmt.replace('INSERT INTO ', 'INSERT OR IGNORE INTO '), row)
            else:
                conn.execute(stmt.replace('UPDATE ', 'UPDATE OR IGNORE '), row)


def _yyyymm_commit(args: dict):
    """The consolidated data is being written into separate DBs like twitter_YYYYMM.db. The data is already split
    into batches separated by YYYYMM key before calling this method. This whole process is done twice: first
    while consolidating data from realDonaldTrump.db and then from BarackObama.db. Consolidating data from the former
    is easy, in that it only involves inserts with no key clashes. Consolidating data from the latter involves
    identifying whether the follower is fully new (only follows BarackObama), else whether or not they have an
    additional status (requires updating that they follow both, and potentially the previous_status column).
    """
    start = time.time()
    root_dir = args['root_dir']
    yyyymm = args['year']
    user_rows = args['user_rows']
    status_rows = args['status_rows']

    with open(os.path.join(root_dir, 'twitter_{}.index'.format(yyyymm)), 'rb') as pfd:
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

    with get_conn(os.path.join(root_dir, 'twitter_{}.db'.format(yyyymm)), read_only=False) as conn:
        conn.execute('pragma synchronous = OFF')
        _executemany(conn, USER_INSERT_STMT, new_user_rows)
        _executemany(conn, USER_UPDATE_STMT_SAME_STATUS, existing_user_rows_with_same_status)
        _executemany(conn, USER_UPDATE_STMT_NEW_STATUS, existing_user_rows_with_new_status)

        if len(new_status_rows) > 0:
            _executemany(conn, STATUS_INSERT_STMT, new_status_rows)

        conn.commit()
    logging.debug('Inserting and committing %s user rows and %s status rows for twitter_%s.db took %s seconds.',
                  len(user_rows), len(status_rows), yyyymm, time.time() - start)


def _parallel_commit_worker(pool_args_list: list):
    for args in pool_args_list:
        _yyyymm_commit(args)


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
        with ThreadPool(len(write_dirs)) as pool:
            pool.map(_parallel_commit_worker, pool_args_lists)
        for year in years:
            user_rows_by_years[year].clear()
            status_rows_by_years[year].clear()
    return time.time() - start


def populate_new_dbs(root_dir: str, username: str, offset: int, write_dirs: list=None):
    """The entry point of the consolidation + splitting process. For each DB, in batches, it fetches user and status
    rows, divides them into buckets of YYYYMMM (follower's creation date), and periodically commits them to the
    respective twitter_YYYYMM.db in parallel. A .tunables.json is used to tweak the batch size for fetching data and
    the threshold size for committing data on the fly. An exit marker is used to gracefully exit in the middle of
    the process. There is no progress saved, so retries start from scratch and the SQL statements are expected to
    handle overwriting."""
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

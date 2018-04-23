"""Layer to store Twitter data in a DB."""
import logging
import os
import sqlite3

from collections import OrderedDict
from typing import List

from twitter.models import User


# Based on python-twitter's User model, with modifications around what and how we want to store the data in SQLite.
USER_COLUMNS = OrderedDict({
    'contributors_enabled': bool,
    'created_at': str,
    'default_profile': bool,
    'default_profile_image': bool,
    'description': str,
    'email': str,
    'favourites_count': int,
    'followers_count': int,
    'friends_count': int,
    'geo_enabled': bool,
    'id_str': str,
    'lang': str,
    'listed_count': int,
    'location': bool,
    'name': str,
    'profile_background_color': str,
    'profile_background_image_url': str,
    'profile_background_tile': bool,
    'profile_banner_url': str,
    'profile_image_url': str,
    'profile_link_color': str,
    'profile_sidebar_fill_color': str,
    'profile_text_color': str,
    'protected': bool,
    'screen_name': str,
    'status': str,
    'statuses_count': int,
    'time_zone': str,
    'url': str,
    'utc_offset': int,
    'verified': bool,
    'withheld_in_countries': str,
    'withheld_scope': str,
})

# Based on python-twitter's Status model, with modifications around what and how we want to store the data in SQLite.
STATUS_COLUMNS = OrderedDict({
    'contributors': str,
    'coordinates': str,
    'created_at': str,
    'geo': str,
    'hashtags': str,
    'id_str': str,
    'in_reply_to_screen_name': str,
    'in_reply_to_status_id': str,
    'in_reply_to_user_id': str,
    'lang': str,
    'place': str,
    'possibly_sensitive': bool,
    'scopes': str,
    'source': str,
    'text': str,
    'withheld_copyright': str,
    'withheld_in_countries': str,
    'withheld_scope': str,
})


def _create_table(conn: sqlite3.Connection, table: str, schema: OrderedDict):
    """Create a table in the DB using the given schema if it doesn't already exist."""
    schema_parts = []
    for key, value in schema.items():
        if value == int:
            schema_parts.append('{} INTEGER'.format(key))
        elif value == bool:
            schema_parts.append('{} BOOLEAN'.format(key))
        elif value == str:
            if key == 'id_str':
                schema_parts.append('{} TEXT PRIMARY KEY'.format(key))
            else:
                schema_parts.append('{} TEXT'.format(key))
        else:
            raise ValueError('Unknown type {} for column {} while creating table {}'.format(value, key, table))

    conn.execute('CREATE TABLE IF NOT EXISTS {} ({})'.format(table, ', '.join(schema_parts)))


def _get_conn(db_path: str, schema: dict, row_factory=None, read_only: bool = True) -> sqlite3.Connection:
    """Get a connection to the DB in the given path. Create schema if necessary."""
    if not os.path.isfile(db_path):
        logging.warning('Could not find an existing DB at %s. Creating one...', db_path)

    if read_only:
        conn = sqlite3.connect('file:{}?mode=ro'.format(db_path), uri=True)
    else:
        conn = sqlite3.connect(db_path)
        for table, defn in schema.items():
            _create_table(conn, table, defn)
        conn.commit()

    if row_factory is not None:
        conn.row_factory = sqlite3.Row

    return conn


def get_conn(db_path: str, read_only: bool = True) -> sqlite3.Connection:
    return _get_conn(db_path, {'users': USER_COLUMNS, 'statuses': STATUS_COLUMNS}, row_factory=None,
                     read_only=read_only)


def _normalize_attr(obj: object, key: str):
    """Normalize an attribute on an object so that it can be saved in the DB."""
    if key not in ['id_str', 'status']:
        attr = getattr(obj, key)
        if attr is None:
            setattr(obj, key, '')
        elif isinstance(attr, dict) or isinstance(attr, list):
            setattr(obj, key, str(attr))


def _insert_stmt(table: str, columns: OrderedDict):
    return 'INSERT INTO {} ({}) VALUES ({})'.format(
        table,
        ', '.join(["'{}'".format(key) for key in columns]),
        ', '.join(['?'] * len(columns)))


def _insert_rows(conn: sqlite3.Connection, table: str, schema: OrderedDict, rows: List[tuple]):
    """Insert a list of rows into the DB's table using the given schema. Does a replace instead of an insert because
    resuming from previous cancellations as well as sharing the DB for storing followers of multiple users would lead
    to duplicate IDs. In addition, the most recent data is being treated as the current/correct data."""
    if len(rows) > 0:
        col_names = ', '.join(["'{}'".format(key) for key in schema])
        col_values = ', '.join(['?'] * len(schema))
        try:
            conn.executemany('REPLACE INTO {} ({}) VALUES ({})'.format(table, col_names, col_values), rows)
        except sqlite3.InterfaceError:
            # To debug some bug that was happening early on. In general, API calls are considered to be more expensive
            # than DB calls.
            for row in rows:
                try:
                    conn.execute('REPLACE INTO {} ({}) VALUES ({})'.format(table, col_names, col_values), row)
                except sqlite3.InterfaceError as e:
                    logging.exception('Error writing %s to table %s.', row, table)
                    raise e


def save_users(conn: sqlite3.Connection, users: List[User]):
    """Save a list of user JSONs in the DB. The JSON includes the most recent publicly available tweet by the user,
    so results in writing to both users and statuses tables."""
    user_rows = []
    status_rows = []
    for user in users:
        row = []
        for key, value in USER_COLUMNS.items():
            _normalize_attr(user, key)
            if key == 'id_str':
                row.append(str(user.id))
            elif key == 'status':
                if user.status is None:
                    row.append('')
                else:
                    row.append(user.status.id_str)
            else:
                row.append(getattr(user, key))
        user_rows.append(tuple(row))

        if user.status is not None:
            if isinstance(user.status.hashtags, list):
                user.status.hashtags = [h.text for h in user.status.hashtags]

            row = []
            for key, value in STATUS_COLUMNS.items():
                _normalize_attr(user.status, key)
                if key in ['in_reply_to_status_id', 'in_reply_to_user_id']:
                    row.append(str(getattr(user.status, key)))
                else:
                    row.append(getattr(user.status, key))
            status_rows.append(tuple(row))

    _insert_rows(conn, 'users', USER_COLUMNS, user_rows)
    _insert_rows(conn, 'statuses', STATUS_COLUMNS, status_rows)
    conn.commit()

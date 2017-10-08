"""Layer to fetch data from Twitter."""
import json
import logging
import os
import pickle
import sqlite3
import time

import twitter

from follower_analyzer import db


CREDS_KEYS = {'consumer_key', 'consumer_secret', 'access_token_key', 'access_token_secret'}


def get_conn(creds_json: str) -> twitter.Api:
    """Get a connection to the Twitter API."""
    if not os.path.isfile(creds_json):
        raise IOError('Could not find credentials at {}.'.format(creds_json))

    with open(creds_json) as cfd:
        creds = json.load(cfd)
        if set(creds.keys()) == CREDS_KEYS:
            return twitter.Api(**creds)
        else:
            raise IOError('%s is expected to have {}.'.format(CREDS_KEYS))


class Progress(object):
    """Class to keep track of progress, backed to a JSON file."""
    def __init__(self, progress_json: str):
        progress_dict = {}
        if os.path.isfile(progress_json):
            with open(progress_json) as pfd:
                progress_dict = json.load(pfd)
        else:
            logging.warning('No progress found at %s. Starting from scratch.', progress_json)
        self.progress_json = progress_json
        self.cursor = progress_dict.get('cursor', -1)
        self.follower_ids = progress_dict.get('follower_ids', [])
        self.last_get_follower_ids = progress_dict.get('last_get_follower_ids', 0)
        self.last_users_lookup = progress_dict.get('last_users_lookup', 0)

    def save(self):
        progress_dict = {'cursor': self.cursor,
                         'follower_ids': self.follower_ids,
                         'last_get_follower_ids': self.last_get_follower_ids,
                         'last_users_lookup': self.last_users_lookup}
        with open(self.progress_json, 'w') as pfd:
            json.dump(progress_dict, pfd)


def save_followers(api: twitter.Api, db_conn: sqlite3.Connection, username: str, progress: Progress, exit_marker: str):
    """Save followers of a Twitter user to DB, keeping track of the progress."""
    now = time.time()
    get_follower_ids_is_rate_limited = now - progress.last_get_follower_ids < 900
    users_lookup_is_rate_limited = now - progress.last_users_lookup < 900
    while True:
        if os.path.exists(exit_marker):
            logging.info('Found exit marker file %s. Stopping.', exit_marker)
            break
        elif not users_lookup_is_rate_limited and len(progress.follower_ids) >= 100:
            logging.info('Getting users.')
            now = time.time()
            users = None
            try:
                users = api.UsersLookup(user_id=progress.follower_ids[:100])
                db.save_users(db_conn, users)
                progress.follower_ids = progress.follower_ids[100:]
            except twitter.error.TwitterError:
                logging.exception('Hit rate-limit while getting users.')
                users_lookup_is_rate_limited = True
            except Exception as e:
                logging.exception('Error writing data to DB. Saving current data to users.dat for investigation.')
                with open('users.dat', 'wb') as pfd:
                    pickle.dump(users, pfd)
                raise e
            finally:
                progress.last_users_lookup = now
                progress.save()
        elif get_follower_ids_is_rate_limited:
            logging.info('Hit rate-limit for getting follower IDs. Sleeping 15 minutes.')
            time.sleep(900)
            get_follower_ids_is_rate_limited = False
            users_lookup_is_rate_limited = False
        elif progress.cursor != 0:
            logging.info('Getting follower IDs.')
            now = time.time()
            try:
                progress.cursor, _previous_cursor, result = \
                        api.GetFollowerIDsPaged(screen_name=username, stringify_ids=True, cursor=progress.cursor)
                progress.follower_ids.extend(result)
                progress.save()
            except twitter.error.TwitterError:
                logging.exception('Hit rate-limit while getting follower IDs.')
                get_follower_ids_is_rate_limited = True
            finally:
                progress.last_get_follower_ids = now
                progress.save()
        else:
            logging.info('Exiting. Rate-limit %s for getting follower IDs, %s for getting users, cursor: %s, '
                         'known follower IDs: %s',
                         'hit' if get_follower_ids_is_rate_limited else 'not hit',
                         'hit' if users_lookup_is_rate_limited else 'not hit',
                         progress.cursor,
                         len(progress.follower_ids))
            break

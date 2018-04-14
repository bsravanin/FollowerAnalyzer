"""Layer to fetch data from Twitter. Requires credentials from a Twitter app with at least read privileges. Currently
only supports a single credential pair. Should be easy and useful to extend it to support an arbitrary number of
credential pairs and rotate across them to minimize sleeps around rate-limits."""
import json
import logging
import os
import pickle
import sqlite3
import time

import twitter

from follower_analyzer import db
from twitter.ratelimit import EndpointRateLimit


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
    """Class to keep track of progress made in collecting data about followers of a user, backed to a JSON file.
    This is useful to resume from any cancellations of the script before it completes."""
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
        self.get_follower_ids_rate_limits = \
            EndpointRateLimit(*progress_dict.get('get_follower_ids_rate_limits', [15, 15, 0]))
        self.users_lookup_rate_limits = \
            EndpointRateLimit(*progress_dict.get('users_lookup_rate_limits', [900, 150, 0]))

    def save(self):
        progress_dict = {'cursor': self.cursor,
                         'follower_ids': self.follower_ids,
                         'get_follower_ids_rate_limits': self.get_follower_ids_rate_limits,
                         'users_lookup_rate_limits': self.users_lookup_rate_limits}
        with open(self.progress_json, 'w') as pfd:
            json.dump(progress_dict, pfd)


def save_followers(api: twitter.Api, db_conn: sqlite3.Connection, username: str, progress: Progress, exit_marker: str):
    """Save followers of a Twitter user to DB, keeping track of the progress. It fetches the list of user_ids of
    followers, and then using those user_ids the profiles of those followers (includes the most recently posted
    tweet if public), while regularly checking rate-limits so as to slow down in advance (using sleeps) instead
    of actually hitting those limits. An exit marker is used to gracefully exit before completion, to be later resumed
    from saved Progress."""
    now = time.time()
    get_follower_ids_is_rate_limited = now > progress.get_follower_ids_rate_limits.reset
    users_lookup_is_rate_limited = now > progress.users_lookup_rate_limits.reset
    while True:
        if os.path.exists(exit_marker):
            logging.info('Found exit marker file %s. Stopping.', exit_marker)
            break
        elif (len(progress.follower_ids) >= 100 or (progress.cursor == 0 and len(progress.follower_ids) > 0)) \
                and not users_lookup_is_rate_limited:
            logging.debug('Getting users.')
            users = None
            try:
                users = api.UsersLookup(user_id=progress.follower_ids[:100])
                db.save_users(db_conn, users)
                progress.follower_ids = progress.follower_ids[100:]
            except twitter.error.TwitterError:
                logging.exception('Hit rate-limit while getting users.')
                users_lookup_is_rate_limited = True
                progress.users_lookup_rate_limits = api.CheckRateLimit('https://api.twitter.com/1.1/users/lookup.json')
            except Exception as e:
                logging.exception('Error writing data to DB. Saving current data to users.dat for investigation.')
                with open('users.dat', 'wb') as pfd:
                    pickle.dump(users, pfd)
                raise e
            finally:
                progress.save()
        elif get_follower_ids_is_rate_limited:
            if len(progress.follower_ids) >= 100 and \
                    progress.get_follower_ids_rate_limits.reset > progress.users_lookup_rate_limits.reset:
                closest_rate_limit_reset = progress.users_lookup_rate_limits.reset
            else:
                closest_rate_limit_reset = progress.get_follower_ids_rate_limits.reset
            duration = max(int(closest_rate_limit_reset - time.time()) + 2, 0)
            logging.info('Hit rate-limits. Sleeping %s seconds.', duration)
            time.sleep(duration)
            get_follower_ids_is_rate_limited = False
            users_lookup_is_rate_limited = False
        elif progress.cursor != 0:
            logging.debug('Getting follower IDs.')
            try:
                progress.cursor, _previous_cursor, result = \
                        api.GetFollowerIDsPaged(screen_name=username, stringify_ids=True, cursor=progress.cursor)
                progress.follower_ids.extend(result)
            except twitter.error.TwitterError:
                logging.exception('Hit rate-limit while getting follower IDs.')
                get_follower_ids_is_rate_limited = True
                progress.get_follower_ids_rate_limits = \
                    api.CheckRateLimit('https://api.twitter.com/1.1/followers/ids.json')
            finally:
                progress.save()
        else:
            logging.info('Exiting. Rate-limit %s for getting follower IDs, %s for getting users, cursor: %s, '
                         'known follower IDs: %s',
                         'hit' if get_follower_ids_is_rate_limited else 'not hit',
                         'hit' if users_lookup_is_rate_limited else 'not hit',
                         progress.cursor,
                         len(progress.follower_ids))
            break

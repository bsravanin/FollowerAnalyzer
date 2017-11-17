"""Layer to fetch data from Twitter."""
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
        self.hashtag_max_ids = progress_dict.get('max_ids', {})

        self.get_follower_ids_rate_limits = \
            EndpointRateLimit(*progress_dict.get('get_follower_ids_rate_limits', [15, 15, 0]))
        self.users_lookup_rate_limits = \
            EndpointRateLimit(*progress_dict.get('users_lookup_rate_limits', [900, 150, 0]))
        self.search_rate_limits = \
            EndpointRateLimit(*progress_dict.get('search_rate_limits', [180, 180, 0]))

    def save(self):
        progress_dict = {'cursor': self.cursor,
                         'follower_ids': self.follower_ids,
                         'hashtag_max_ids': self.hashtag_max_ids,
                         'get_follower_ids_rate_limits': self.get_follower_ids_rate_limits,
                         'users_lookup_rate_limits': self.users_lookup_rate_limits,
                         'search_rate_limits': self.search_rate_limits}
        with open(self.progress_json, 'w') as pfd:
            json.dump(progress_dict, pfd)


def save_followers(api: twitter.Api, db_conn: sqlite3.Connection, username: str, progress: Progress, exit_marker: str):
    """Save followers of a Twitter user to DB, keeping track of the progress."""
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


def search(api: twitter.Api, db_conn: sqlite3.Connection, hashtags: list, progress: Progress, exit_marker: str,
           result_type: str='popular'):
    """Search popular tweets with given hashtags and save them in their respective DBs, keeping track of progress."""
    now = time.time()
    search_is_rate_limited = now > progress.search_rate_limits.reset
    index = 0
    while True:
        if len(hashtags) == 0:
            logging.info('No hashtags to search. Stopping.')
            break
        elif os.path.exists(exit_marker):
            logging.info('Found exit marker file %s. Stopping.', exit_marker)
            break
        elif not search_is_rate_limited:
            if index >= len(hashtags) or index < 0:
                index %= len(hashtags)
            current_hashtag = hashtags[index]
            index += 1
            logging.debug('Searching Twitter for %s.', current_hashtag)
            try:
                tweets = api.GetSearch(count=100, result_type=result_type, term=current_hashtag,
                                       max_id=progress.hashtag_max_ids.get(current_hashtag),
                                       include_entities=True)
                db.save_tweets(db_conn, tweets)

                if len(tweets) < 100:
                    logging.info('Retrieved all %s tweets for hashtag %s. Removing it from list.',
                                 result_type, current_hashtag)
                    hashtags.remove(current_hashtag)
                    index -= 1

                if len(tweets) > 0:
                    progress.hashtag_max_ids[current_hashtag] = tweets[-1].id - 1
            except twitter.error.TwitterError:
                logging.exception('Hit rate-limit while searching Twitter.')
                search_is_rate_limited = True
                progress.search_rate_limits = api.CheckRateLimit('https://api.twitter.com/1.1/search/tweets.json')
            finally:
                progress.save()
        elif search_is_rate_limited:
            closest_rate_limit_reset = progress.search_rate_limits.reset
            duration = max(int(closest_rate_limit_reset - time.time()) + 2, 0)
            logging.info('Hit rate-limits. Sleeping %s seconds.', duration)
            time.sleep(duration)
            search_is_rate_limited = False

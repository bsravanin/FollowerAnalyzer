#! /usr/bin/env python3

import argparse
import logging
import os
import time

from follower_analyzer import db
from follower_analyzer import twitter_api


LOGGING_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOGGING_LEVEL = logging.INFO


def _create_parser():
    parser = argparse.ArgumentParser(description='Interface for Twitter follower analysis.')
    parser.add_argument('-c', '--credentials', action='store', default='.twitter.json',
                        help='Path to JSON containing Twitter API credentials. Defaults to ./.twitter.json.')
    parser.add_argument('-d', '--dbpath', action='store', required=True,
                        help='Path to DB to be used. Create if necessary.')
    parser.add_argument('-e', '--exit_marker', action='store', default='./exit_follower_analyzer',
                        help='Path to a file that is checked for premature exit. Defaults to ./exit_follower_analyzer.')
    parser.add_argument('--hashtags', action='store',
                        help='File containing a list of hashtags to be retrieved.')
    parser.add_argument('-i', '--ignore_exceptions', action='store_true',
                        help='Ignore exceptions. Useful for long unsupervised sessions.')
    parser.add_argument('-p', '--progress', action='store', default='.progress.json',
                        help='Path to JSON containing previous progress to pickup from. Defaults to ./.progress.json.')
    parser.add_argument('-u', '--username', action='store',
                        help='Twitter user whose followers are to be analyzed.')
    return parser.parse_args()


def main():
    """Main function that will be run if this is used as a script instead of imported."""
    logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
    args = _create_parser()

    if (args.username is None and args.hashtags is None) or (args.username is not None and args.hashtags is not None):
        logging.error('Exactly one of username or hashtags is required as an option')
        return

    progress = twitter_api.Progress(args.progress)
    twitter_conn = twitter_api.get_conn(args.credentials)
    if args.username is not None:
        db_conn = db.get_conn(args.dbpath, read_only=False)
        while True:
            try:
                twitter_api.save_followers(twitter_conn, db_conn, args.username, progress, args.exit_marker)
                break
            except:
                if args.ignore_exceptions:
                    logging.info('Caught an unknown exception. Will retry after sleeping for 300s.')
                    time.sleep(300)
                else:
                    raise
    elif args.hashtags is not None:
        if not os.path.isfile(args.hashtags):
            logging.error('Could not read file %s.', args.hashtags)
            return

        with open(args.hashtags) as hfd:
            hashtags = set()
            for line in hfd:
                hashtag = line.strip()
                if hashtag != '':
                    hashtags.add(hashtag)
                    hashtags.add('#{}'.format(hashtag.replace(' ', '')))

        db_conn = db.get_conn(args.dbpath, read_only=False, full_status=True)

        for result_type in ['recent', 'mixed', 'popular']:
            while True:
                try:
                    twitter_api.search(twitter_conn, db_conn, sorted(list(hashtags)), progress, args.exit_marker,
                                       result_type=result_type)
                    break
                except:
                    if args.ignore_exceptions:
                        logging.exception('Caught an unknown exception. Will retry after sleeping for 300s.')
                        time.sleep(300)
                    else:
                        raise


if __name__ == '__main__':
    main()

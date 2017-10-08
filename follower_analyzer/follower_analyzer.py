#! /usr/bin/env python3

import argparse
import logging

from follower_analyzer import db
from follower_analyzer import twitter_api


LOGGING_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOGGING_LEVEL = logging.INFO


def _create_parser():
    parser = argparse.ArgumentParser(description='Interface for Twitter follower analysis.')
    parser.add_argument('-c', '--credentials', action='store', default='.twitter.json',
                        help='Path to JSON containing Twitter API credentials. Defaults to ./.twitter.json.')
    parser.add_argument('-d', '--dbpath', action='store', default='twitter.db',
                        help='Path to DB to be used. Create if necessary. Defaults to ./twitter.db.')
    parser.add_argument('-e', '--exit_marker', action='store', default='./exit_follower_analyzer',
                        help='Path to a file that is checked for premature exit. Defaults to ./exit_follower_analyzer.')
    parser.add_argument('-p', '--progress', action='store', default='.progress.json',
                        help='Path to JSON containing previous progress to pickup from. Defaults to ./.progress.json.')
    parser.add_argument('-u', '--username', action='store', required=True,
                        help='Twitter user whose followers are to be analyzed.')
    return parser.parse_args()


def main():
    """Main function that will be run if this is used as a script instead of imported."""
    logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)
    args = _create_parser()

    progress = twitter_api.Progress(args.progress)
    twitter_conn = twitter_api.get_conn(args.credentials)
    db_conn = db.get_conn(args.dbpath)
    twitter_api.save_followers(twitter_conn, db_conn, args.username, progress, args.exit_marker)


if __name__ == '__main__':
    main()

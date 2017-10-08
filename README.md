FollowerAnalyzer
================
A CLI tool that uses [python-twitter](https://github.com/bear/python-twitter)
to fetch information about all followers of a Twitter user, and stores them in an SQLite DB.

It has been written with the most popular Twitter celebrities in mind, as a long-running program:
* Handles Twitter rate-limits well.
* Stores data in a DB.
* Keeps track of progress at a granular level so that the program can be stopped and resumed
  without making duplicate Twitter API calls. (It is not optimized to minimize disk IO.)

SECURITY
========
When creating credentials for your [Twitter App](https://apps.twitter.com) make sure to get
read-only access.

Save the credentials locally in a JSON like:
{
    "consumer_key": "YOUR_CONSUMER_KEY",
    "consumer_secret": "YOUR_CONSUMER_SECRET",
    "access_token_key": "YOUR_ACCESS_TOKEN_KEY",
    "access_token_secret": "YOUR_ACCESS_TOKEN_SECRET"
}

Remember to not commit/upload these credentials anywhere.

TODO
====
* Add few useful functions for actual analysis of data.
* Add support for storing data about more than one user. (Currently, all data in a DB is expected
  to belong to a single Twitter user. Consequently cross-user analysis gets complicated.)

"""Collection of various functions used to analyze data."""
import logging
import os
import pickle
import time

from collections import Counter
from collections import defaultdict

from openpyxl import Workbook

from follower_analyzer.db import STATUS_COLUMNS
from follower_analyzer import db2
from follower_analyzer.db2 import USER_COLUMNS2


OBAMA = 'BarackObama'
TRUMP = 'realDonaldTrump'
BOTH = 'BOTH'


def _save_data(fpath: str, obj: object):
    with open(fpath, 'wb') as pfd:
        pickle.dump(obj, pfd)


def collect_counters(dbpath: str, max_iterations: int=1000, bucket: int=10, query_limit: int=100000):
    counters = {OBAMA: defaultdict(Counter), BOTH: defaultdict(Counter), TRUMP: defaultdict(Counter)}
    start = time.time()
    statuses = {BOTH: set(), TRUMP: set()}
    conn = db2.get_conn(dbpath)
    cursor = conn.cursor()
    cursor.execute('select * from users')
    index = 0
    while True:
        if index == max_iterations:
            break
        else:
            index += 1
        rows = cursor.fetchmany(query_limit)
        for row in rows:
            following = row['following']
            if following == BOTH:
                names = [OBAMA, TRUMP, BOTH]
            else:
                names = [following]

            if following != OBAMA:
                if row['status'] != '':
                    statuses[following].add(row['status'])
                if row['previous_status'] != '':
                    statuses[following].add(row['previous_status'])

            for name in names:
                for k, v in USER_COLUMNS2.items():
                    if k in ['location', 'following']:
                        # TODO: Some of these need special handling.
                        pass
                    elif v == bool or k in ['utc_offset', 'lang'] or k.endswith('_color') or \
                            k.startswith('withheld_'):
                        counters[name][k][row[k]] += 1
                    elif v == int:
                        if row[k] < 10:
                            counters[name][k][row[k]] += 1
                        else:
                            counters[name][k][(row[k] // bucket) * bucket] += 1
                    elif k == 'created_at':
                        date = time.strftime('%Y%m%d', time.strptime(row[k], '%a %b %d %H:%M:%S +0000 %Y'))
                        counters[name][k][date] += 1
                    elif k in ['status', 'previous_status']:
                        if row[k] != '':
                            counters[name][k][1] += 1
                        else:
                            counters[name][k][0] += 1

                followers = row['followers_count']
                friends = row['friends_count']
                if followers > 0 and len(counters[name]['friends_to_followers']) < 10000:
                    ratio = friends / followers
                    if ratio < 1:
                        counters[name]['friends_to_followers'][round(ratio, 1)] += 1
                    else:
                        counters[name]['friends_to_followers'][round(ratio)] += 1
                if friends > 0 and len(counters[name]['followers_to_friends']) < 10000:
                    ratio = followers / friends
                    if ratio < 1:
                        counters[name]['followers_to_friends'][round(ratio, 1)] += 1
                    else:
                        counters[name]['followers_to_friends'][round(ratio)] += 1

        if len(rows) < query_limit:
            break

    cursor.execute('select * from statuses')
    index = 0
    while True:
        if index == max_iterations:
            break
        else:
            index += 1
        rows = cursor.fetchmany(query_limit)
        for row in rows:
            if row['id_str'] in statuses[BOTH]:
                names = [OBAMA, TRUMP, BOTH]
            elif row['id_str'] in statuses[TRUMP]:
                names = [TRUMP]
            else:
                names = [OBAMA]

            for name in names:
                for k, v in STATUS_COLUMNS.items():
                    if k in ['contributors', 'lang', 'possibly_sensitive', 'scopes', 'withheld_copyright',
                             'withheld_in_countries', 'withheld_scope']:
                        counters[name]['status_{}'.format(k)][row[k]] += 1
                    elif k == 'created_at':
                        date = time.strftime('%Y%m%d', time.strptime(row[k], '%a %b %d %H:%M:%S +0000 %Y'))
                        counters[name]['status_{}'.format(k)][date] += 1

        if len(rows) < query_limit:
            break

    _save_data(dbpath.replace('.db', '.dat'), counters)
    logging.debug('Collecting various counts of users and statuses in %s took %s seconds using %s path.',
                  os.path.basename(dbpath), time.time() - start, 'fast' if 'sravan' in dbpath else 'slow')


def _load_data(fpath: str):
    with open(fpath, 'rb') as pfd:
        return pickle.load(pfd)


def merge_counters(counter_paths: list, merged_counter_path: str):
    counters = {OBAMA: defaultdict(Counter), BOTH: defaultdict(Counter), TRUMP: defaultdict(Counter)}
    for cpath in counter_paths:
        start = time.time()
        for name, breakdown in _load_data(cpath).items():
            for column, column_counter in breakdown.items():
                for k, v in column_counter.items():
                    counters[name][column][k] += v
        logging.debug('Merging various counts of users and statuses in %s took %s seconds using %s path.',
                      os.path.basename(cpath), time.time() - start, 'fast' if 'sravan' in cpath else 'slow')

    _save_data(merged_counter_path, counters)


def _get_key_range(counters: dict, key: str):
    if key.endswith('_color') or key.endswith('lang'):
        return [mc[0] for mc in counters[BOTH][key].most_common(1000)]
    else:
        key_range = set()
        for name in counters:
            key_range |= set(counters[name][key].keys())
        if '' in key_range:
            key_range.remove('')
            return [''] + sorted(key_range)
        else:
            return sorted(key_range)


def _get_title(key: str):
    return key.replace('_', ' ').title()


def _set_column(sheet, column: int, values: list):
    for index, val in enumerate(values):
        sheet.cell(row=index+1, column=column).value = val


def _set_row(sheet, row:int, values: tuple):
    for index, val in enumerate(values):
        sheet.cell(row=row, column=index+1).value = val


def _fill_sheet(sheet, counters: dict, keys: list):
    index = 1
    for key in keys:
        key_range = _get_key_range(counters, key)
        _set_column(sheet, index, [_get_title(key)] + key_range)
        for nindex, name in enumerate([OBAMA, TRUMP, BOTH]):
            _set_column(sheet, index+1+nindex, [name] + [counters[name][key][k] for k in key_range])
        index += 5


def write_report(merged_counter_path: str, report_path: str):
    counters = _load_data(merged_counter_path)

    workbook = Workbook()

    counts_sheet = workbook.active
    counts_sheet.title = 'Counts'
    _fill_sheet(counts_sheet, counters, ['favourites_count', 'followers_count', 'friends_count', 'listed_count',
                                         'statuses_count'])

    colors_sheet = workbook.create_sheet('Colors')
    _fill_sheet(colors_sheet, counters, ['profile_background_color', 'profile_link_color', 'profile_sidebar_fill_color',
                                         'profile_text_color'])

    time_sheet = workbook.create_sheet('Times')
    _fill_sheet(time_sheet, counters, ['created_at', 'status_created_at', 'utc_offset'])

    lang_sheet = workbook.create_sheet('Languages')
    _fill_sheet(lang_sheet, counters, ['lang', 'status_lang'])

    ratios_sheet = workbook.create_sheet('Ratios')
    _fill_sheet(ratios_sheet, counters, ['followers_to_friends', 'friends_to_followers'])

    flags_sheet = workbook.create_sheet('Flags & Miscellaneous')
    _set_row(flags_sheet, 1, ('Flag', 'Value', OBAMA, TRUMP, BOTH))
    index = 2
    for key in ['contributors_enabled', 'default_profile', 'default_profile_image', 'geo_enabled',
                'profile_background_tile', 'protected', 'verified', 'status', 'previous_status',
                'status_possibly_sensitive', 'status_contributors', 'status_scopes', 'status_withheld_copyright',
                'status_withheld_in_countries', 'status_withheld_scope', 'withheld_in_countries', 'withheld_scope']:
        for val in _get_key_range(counters, key):
            _set_row(flags_sheet, index, (_get_title(key), val, counters[OBAMA][key][val], counters[TRUMP][key][val],
                                          counters[BOTH][key][val]))
            index += 1

    workbook.save(report_path)

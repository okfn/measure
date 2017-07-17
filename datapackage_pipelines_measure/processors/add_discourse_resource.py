import collections
import datetime
import dateutil
import urllib

import simplejson
import requests

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

DEFAULT_REPORT_START_DATE = '2014-01-01'


def _request_data_from_discourse(domain, endpoint, **kwargs):
    api_token = settings['DISCOURSE_API_TOKEN']
    qs_dict = {'api_key': api_token}
    qs_dict.update(kwargs)
    qs = urllib.parse.urlencode(qs_dict)
    url = urllib.parse.urlunparse(
        ('https', domain, endpoint, None, qs, None)
    )
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(
            'Error raised for domain:{}, '
            'Status code:{}. '
            'Error message: {}'.format(domain,
                                       response.status_code,
                                       response.content))

    try:
        json_response = response.json()
    except simplejson.scanner.JSONDecodeError as e:
        log.error('Expected JSON in response from: {}'.format(url))
        raise ValueError('Expected JSON in response from: {}'.format(url))

    return json_response


def _request_users_from_discourse(domain, flag, page=1):
    endpoint = "/admin/users/list/{}.json".format(flag)
    return _request_data_from_discourse(domain, endpoint, page=page)


def _request_report_from_discourse(domain, report, start_date, end_date=None):
    '''Request a report from discourse and return a dict of <date: count> key
    values.'''
    if end_date is None:
        end_date = datetime.date.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = DEFAULT_REPORT_START_DATE
    endpoint = "/admin/reports/{}.json".format(report)
    data = _request_data_from_discourse(domain, endpoint,
                                        start_date=start_date,
                                        end_date=end_date,
                                        category_id='all')['report']['data']
    return {dateutil.parser.parse(d['x']).date(): d['y'] for d in data}


def _get_active_users_number_last_24_hrs(domain):
    '''Return the number of active users within last 24hrs.'''

    def _page_active_users(domain):
        '''Request active users by page until an empty array is return, or we
        reach a user created after the last 24hr'''
        current_page = 1  # /admin/users/list paging starts at one
        while True:
            users = _request_users_from_discourse(domain, 'active',
                                                  current_page)
            if len(users) == 0:
                raise StopIteration
            current_page = current_page + 1
            for user in users:
                if not user['last_seen_age'].endswith(('h', 'm')):
                    raise StopIteration
                yield user

    return len(list(_page_active_users(domain)))


def discourse_collector(domain, latest_row):
    today = datetime.date.today()
    latest_date = latest_row['date'] if latest_row else None
    active_users_response = _get_active_users_number_last_24_hrs(domain)
    new_users_by_date = \
        _request_report_from_discourse(domain, 'signups', latest_date)
    new_topics_by_date = \
        _request_report_from_discourse(domain, 'topics', latest_date)
    new_posts_by_date = \
        _request_report_from_discourse(domain, 'posts', latest_date)
    visits_by_date = \
        _request_report_from_discourse(domain, 'visits', latest_date)

    dd = collections.defaultdict(lambda: {'new_users': 0,
                                          'new_topics': 0,
                                          'new_posts': 0,
                                          'visits': 0})
    for date, user_num in new_users_by_date.items():
        dd[date]['new_users'] = user_num

    for date, topic_num in new_topics_by_date.items():
        dd[date]['new_topics'] = topic_num

    for date, post_num in new_posts_by_date.items():
        dd[date]['new_posts'] = post_num

    for date, visits_num in visits_by_date.items():
        dd[date]['visits'] = visits_num

    # We want today to exist, if it doesn't, so we can add active_users to it.
    # Given the magic of defaultdict, to access is to create.
    dd[today]

    resource_content = []
    for date, stats in dd.items():
        res_row = {
            'source': 'discourse',
            'domain': domain,
            'date': date,
            'new_users': stats['new_users'],
            'new_topics': stats['new_topics'],
            'new_posts': stats['new_posts'],
            'visits': stats['visits']
        }
        # add active_users to today's value
        if date == today:
            res_row['active_users'] = active_users_response
        # preserve active_users value in latest_row
        if date == latest_date and latest_row['active_users']:
            res_row['active_users'] = latest_row['active_users']
        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

domain = parameters['domain']
resource = {
    'name': slugify(domain),
    'path': 'data/{}.csv'.format(slugify(domain))
}

headers = ['domain', 'source', 'date', 'new_users', 'new_topics',
           'new_posts', 'visits', 'active_users']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, domain):

    def get_latest_row(first):
        latest_row = None
        my_rows = []
        for row in first:
            if row['domain'] == domain and row['source'] == 'discourse':
                latest_row = row
            my_rows.append(row)
        return latest_row, iter(my_rows)

    if len(datapackage['resources']):
        if datapackage['resources'][0]['name'] == 'latest-project-entries':
            latest_row, latest_iter = get_latest_row(next(res_iter))
            yield latest_iter
        else:
            latest_row = None
    yield from res_iter
    yield discourse_collector(domain, latest_row)


spew(datapackage, process_resources(res_iter, datapackage, domain))

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


def _request_topics_from_discourse(domain, page=1):
    endpoint = "/latest.json"
    response = _request_data_from_discourse(domain, endpoint,
                                            page=page, order='created')
    return response['topic_list']['topics']


def _get_new_topics_by_date(domain, start_date):
    '''Return a Counter where the key is a date, and value is the number of new
    topics for that day.'''

    def _page_new_topics(domain, start_date):
        '''Request new users by page until an empty array is returned, or we
        reach a user created before the start_date.'''
        current_page = 1
        while True:
            users = _request_topics_from_discourse(domain, current_page)
            if len(users) == 0:
                # Nothing returned for this page, we're done.
                raise StopIteration
            current_page = current_page + 1
            user_dates = [dateutil.parser.parse(u['created_at']).date()
                          for u in users]

            for user_date in sorted(user_dates, reverse=True):
                # We're reached before the start_date, we're done.
                if start_date and user_date < start_date:
                    raise StopIteration
                yield user_date

    return collections.Counter(_page_new_topics(domain, start_date))


def _get_new_users_number_by_date(domain, start_date):
    '''Return a Counter where the key is a date, and value is the number of new
    users for that day.'''

    def _page_new_users(domain, start_date):
        '''Request new users by page until an empty array is returned, or we
        reach a user created before the start_date.'''
        current_page = 1
        while True:
            users = _request_users_from_discourse(domain, 'new', current_page)
            if len(users) == 0:
                # Nothing returned for this page, we're done.
                raise StopIteration
            current_page = current_page + 1
            user_dates = [dateutil.parser.parse(u['created_at']).date()
                          for u in users]

            for user_date in sorted(user_dates, reverse=True):
                # We're reached before the start_date, we're done.
                if start_date and user_date < start_date:
                    raise StopIteration
                yield user_date

    return collections.Counter(_page_new_users(domain, start_date))


def _get_active_users_number_last_24_hrs(domain):
    '''Return the number of active users within last 24hrs.'''

    def _page_active_users(domain):
        '''Request active users by page until an empty array is return, or we
        reach a user created after the last 24hr'''
        current_page = 1
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
    new_users_by_date = _get_new_users_number_by_date(domain, latest_date)
    active_users_response = _get_active_users_number_last_24_hrs(domain)
    new_topics_by_date = _get_new_topics_by_date(domain, latest_date)

    dd = collections.defaultdict(lambda: {'new_users': 0, 'new_topics': 0})
    for date, user_num in new_users_by_date.items():
        dd[date]['new_users'] = user_num

    for date, topic_num in new_topics_by_date.items():
        dd[date]['new_topics'] = topic_num

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
            'new_topics': stats['new_topics']
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
           'active_users']
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

import datetime
import dateutil
import json

import requests
from requests.auth import HTTPBasicAuth

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)


def _request_data_from_mailchimp(endpoint):
    '''Request data and handle errors from MailChimp REST API.'''
    api_token = settings['MAILCHIMP_API_TOKEN']
    data_center = api_token.split('-')[-1]
    mailchimp_url = 'https://{dc}.api.mailchimp.com/3.0{endpoint}' \
        .format(dc=data_center, endpoint=endpoint)

    mailchimp_response = requests.get(mailchimp_url,
                                      auth=HTTPBasicAuth('username',
                                                         api_token))

    if (mailchimp_response.status_code != 200):
        log.error('An error occurred fetching MailChimp data: {}'
                  .format(mailchimp_response.json()['detail']))
        raise Exception(mailchimp_response.json()['detail'])

    try:
        json_response = mailchimp_response.json()
    except json.decoder.JSONDecodeError as e:
        log.error('Expected JSON in response from: {}'.format(mailchimp_url))
        raise e

    return json_response


def _request_general_stats_from_mailchimp(list_id):
    '''Request general list data from MailChimp.'''

    endpoint = '/lists/{list_id}'.format(list_id=list_id)
    json_response = _request_data_from_mailchimp(endpoint)

    return json_response


def _request_activity_stats_from_mailchimp(list_id, count=None):
    '''Request activity for the list from MailChimp'''
    endpoint = '/lists/{list_id}/activity'.format(list_id=list_id)
    if count:
        endpoint = '{}?count={}'.format(endpoint, count)
    json_response = _request_data_from_mailchimp(endpoint)

    return json_response


def _get_start_date(default_start, latest_date=None):
    '''Determine when data collection should start.

    :latest_date: the most recent date data was collected for this list_id, if
        it exists
    '''
    if latest_date:
        return max(latest_date, default_start)
    else:
        return default_start


def mailchimp_collector(list_id, latest_date):
    general_stats = _request_general_stats_from_mailchimp(list_id)
    list_created = dateutil.parser.parse(general_stats['date_created']).date()

    start_date = _get_start_date(list_created, latest_date)
    delta = datetime.date.today() - start_date
    day_count = delta.days

    activity_stats = _request_activity_stats_from_mailchimp(list_id,
                                                            count=day_count)

    resource_content = []
    for activity in activity_stats['activity']:
        activity_date = dateutil.parser.parse(activity['day']).date()
        res_row = {
            'source': 'mailchimp',
            'list_id': list_id,
            'date': activity_date,
            'subs': activity['subs'],
            'unsubs': activity['unsubs']
        }
        # If date of activity is today, add the subscribers data from general
        # stats.
        if activity_date == datetime.date.today():
            res_row['subscribers'] = general_stats['stats']['member_count']
        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

list_id = parameters['list_id']
resource = {
    'name': slugify(list_id),
    'path': 'data/{}.csv'.format(slugify(list_id))
}

headers = ['source', 'date', 'list_id', 'subs', 'unsubs', 'subscribers']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, list_id):

    def get_latest_date(first):
        latest_date = None
        my_rows = []
        for row in first:
            if row['list_id'] == list_id and row['source'] == 'mailchimp':
                latest_date = row['date']
            my_rows.append(row)
        return latest_date, iter(my_rows)

    if len(datapackage['resources']):
        if datapackage['resources'][0]['name'] == 'latest-project-entries':
            latest_date, latest_iter = get_latest_date(next(res_iter))
            yield latest_iter
        else:
            latest_date = None
    yield from res_iter
    yield mailchimp_collector(list_id, latest_date)


spew(datapackage, process_resources(res_iter, datapackage, list_id))

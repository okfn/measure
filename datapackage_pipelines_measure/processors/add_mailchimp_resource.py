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


def mailchimp_collector(list_id, latest_row):
    general_stats = _request_general_stats_from_mailchimp(list_id)
    list_created = dateutil.parser.parse(general_stats['date_created']).date()

    latest_date = latest_row['date'] if latest_row else None
    start_date = _get_start_date(list_created, latest_date)
    delta = datetime.date.today() - start_date
    # Count the number of days from the start_date to today. Add an extra day
    # to include the previous entry, which already exists in the db. We want to
    # update its `subs` and `unsubs` but retain its `subscribers` value.
    day_count = delta.days + 1

    activity_stats = _request_activity_stats_from_mailchimp(list_id,
                                                            count=day_count)

    resource_content = []
    for activity in activity_stats['activity']:
        activity_date = dateutil.parser.parse(activity['day']).date()
        res_row = {
            'source': 'mailchimp',
            'list_id': list_id,
            'date': activity_date,
            'subs': activity['subs'] + activity['other_adds'],
            'unsubs': activity['unsubs'] + activity['other_removes']
        }
        # If date of activity is today, add the subscribers data from general
        # stats.
        if activity_date == datetime.date.today():
            res_row['subscribers'] = general_stats['stats']['member_count']
        # If date of activity is the latest existing row, add its subscribers
        # value to the new row, retaining it when updated to the db.
        if activity_date == latest_date:
            res_row['subscribers'] = latest_row['subscribers']
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

    def get_latest_row(first):
        latest_row = None
        my_rows = []
        for row in first:
            if row['list_id'] == list_id and row['source'] == 'mailchimp':
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
    yield mailchimp_collector(list_id, latest_row)


spew(datapackage, process_resources(res_iter, datapackage, list_id))

import collections
import calendar
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


def _request_activity_stats_from_mailchimp(list_id, count):
    '''Request activity for the list_id from MailChimp.'''
    endpoint = '/lists/{list_id}/activity?count={count}' \
        .format(list_id=list_id, count=count)
    json_response = _request_data_from_mailchimp(endpoint)

    return json_response


def _request_campaign_stats_from_mailchimp(list_id, since):
    '''Request campaign stats for the list_id from MailChimp, where the
    send_time is after `since` (inclusive).'''
    endpoint = '/campaigns/?list_id={list_id}&since_send_time={since}' \
        .format(list_id=list_id, since=since)
    json_response = _request_data_from_mailchimp(endpoint)

    return json_response


def _request_growth_history_from_mailchimp(list_id, year_month):
    '''Request growth-history for a give 'yyyy-mm' from MailChimp.'''
    endpoint = '/lists/{list_id}/growth-history/{year_month}' \
        .format(list_id=list_id, year_month=year_month)
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


def _get_campaigns_number_by_date(list_id, start_date):
    '''Return a Counter where the key is a date, and value is the number of
    campaigns sent on that date'''
    campaigns = _request_campaign_stats_from_mailchimp(list_id, start_date)
    campaigns_sent = [dateutil.parser.parse(c['send_time']).date()
                      for c in campaigns['campaigns']]
    return collections.Counter(campaigns_sent)


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

    # Get campaign stats for activity_date as a Counter({date obj: integer})
    campaigns_dates = _get_campaigns_number_by_date(list_id, start_date)

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
        # If date of activity is also the latest existing row, add its
        # subscribers value to the new row, retaining it when updated to db.
        if activity_date == latest_date:
            res_row['subscribers'] = latest_row['subscribers']
        # Add number of campaigns sent from `campaigns_dates`.
        res_row['campaigns_sent'] = campaigns_dates.get(activity_date, 0)
        # We can collect historical `subscribers` data from MailChimp for the
        # last day of each month. Let's do that if activity_date is the last in
        # month, and we haven't already populated the value above.
        activity_month_range = calendar.monthrange(activity_date.year,
                                                   activity_date.month)
        if activity_date.day == activity_month_range[1] \
           and 'subscribers' not in res_row:
            growth = _request_growth_history_from_mailchimp(
                list_id,
                '{}-{:02d}'.format(activity_date.year, activity_date.month)
            )
            res_row['subscribers'] = growth['existing']

        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

list_id = parameters['list_id']
resource = {
    'name': slugify(list_id),
    'path': 'data/{}.csv'.format(slugify(list_id))
}

headers = ['source', 'date', 'list_id', 'subs', 'unsubs', 'subscribers',
           'campaigns_sent']
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

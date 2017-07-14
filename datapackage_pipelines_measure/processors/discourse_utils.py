import datetime
import dateutil
import urllib

import requests
import simplejson

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

DEFAULT_REPORT_START_DATE = '2014-01-01'


def request_data_from_discourse(domain, endpoint, **kwargs):
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


def request_report_from_discourse(domain, report, start_date,
                                  category_id='all', end_date=None):
    '''Request a report from discourse and return a dict of <date: count> key
    values.'''
    if end_date is None:
        end_date = datetime.date.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = DEFAULT_REPORT_START_DATE
    endpoint = "/admin/reports/{}.json".format(report)
    data = request_data_from_discourse(
        domain, endpoint,
        start_date=start_date,
        end_date=end_date,
        category_id=category_id)['report']['data']
    return {dateutil.parser.parse(d['x']).date(): d['y'] for d in data}

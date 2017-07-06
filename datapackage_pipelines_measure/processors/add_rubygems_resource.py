import datetime

import simplejson
import requests

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

import logging
log = logging.getLogger(__name__)


def _request_data_from_rubygems(endpoint):
    '''Request data and handle errors from rubygems.org REST API.'''

    rubygems_url = 'https://rubygems.org/api/v1{endpoint}' \
        .format(endpoint=endpoint)

    rubygems_response = requests.get(rubygems_url)

    if (rubygems_response.status_code != 200):
        log.error('An error occurred fetching Rubygems data: {}'
                  .format(rubygems_response.text))
        raise Exception(rubygems_response.text)

    try:
        json_response = rubygems_response.json()
    except simplejson.scanner.JSONDecodeError as e:
        log.error('Expected JSON in response from: {}'.format(rubygems_url))
        raise e

    return json_response


def _request_gem_stats_from_rubygems(gem_id):
    '''Request general info for a gem_id.'''
    endpoint = '/gems/{gem_id}.json'.format(gem_id=gem_id)
    json_response = _request_data_from_rubygems(endpoint)
    return json_response


def rubygems_collector(gem_id, latest_row):
    gem_info = _request_gem_stats_from_rubygems(gem_id)

    total_downloads = gem_info['downloads']

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    res_row = {
        'total_downloads': total_downloads,
        'date': today,
        'package': gem_id,
        'source': 'rubygems'
    }

    # Calculate daily downloads from total_downloads.
    if latest_row:
        if latest_row['date'] == yesterday:
            res_row['downloads'] = \
                total_downloads - latest_row['total_downloads']
        # If latest is today, retain `downloads` value.
        elif latest_row['date'] == today:
            res_row['downloads'] = latest_row['downloads']

    resource_content = []
    resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

gem_id = parameters['gem_id']
resource = {
    'name': slugify(gem_id),
    'path': 'data/{}.csv'.format(slugify(gem_id))
}

headers = ['source', 'date', 'package', 'downloads', 'total_downloads']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, gem_id):

    def get_latest_row(first):
        latest_row = None
        my_rows = []
        for row in first:
            if row['package'] == gem_id and row['source'] == 'rubygems':
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
    yield rubygems_collector(gem_id, latest_row)


spew(datapackage, process_resources(res_iter, datapackage, gem_id))

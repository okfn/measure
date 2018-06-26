import dateutil
from collections import OrderedDict

import simplejson
import requests

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

import logging
log = logging.getLogger(__name__)


def _request_data_from_packagist(endpoint):
    '''Request data and handle errors from packagist.org REST API.'''

    packagist_url = 'https://packagist.org{endpoint}' \
        .format(endpoint=endpoint)

    packagist_response = requests.get(packagist_url)

    if (packagist_response.status_code != 200):
        log.error('An error occurred fetching Packagist data: {}'
                  .format(packagist_response.text))
        raise Exception(packagist_response.text)

    try:
        json_response = packagist_response.json()
    except simplejson.scanner.JSONDecodeError as e:
        log.error('Expected JSON in response from: {}'.format(packagist_url))
        raise e

    return json_response


def _request_package_stats_from_packagist(package):
    '''Request general info for a package.'''
    endpoint = '/packages/{package}/stats/all.json'.format(package=package)
    json_response = _request_data_from_packagist(endpoint)
    return json_response


def packagist_collector(package, latest_row):
    package_info = _request_package_stats_from_packagist(package)
    download_by_date = dict(zip(package_info['labels'],
                                package_info['values']))

    if latest_row:
        # If there's a latest_row, reject all items in download_by_date before
        # latest_row date
        latest_row_date_str = latest_row['date'].strftime('%Y-%m-%d')
        download_by_date = {k: v for k, v in download_by_date.items()
                            if k >= latest_row_date_str}

    # ensure dict is ordered by date key
    download_by_date = OrderedDict(sorted(download_by_date.items()))

    resource_content = []
    for k, v in download_by_date.items():
        res_row = {
            'package': package.split('/')[-1],
            'source': 'packagist',
            'date': dateutil.parser.parse(k).date(),
            'downloads': v
        }
        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

package = parameters['package']
resource = {
    'name': slugify(package),
    'path': 'data/{}.csv'.format(slugify(package)),
    PROP_STREAMING: True
}

headers = ['package', 'source', 'date', 'downloads']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, package):

    def get_latest_row(first):
        latest_row = None
        package_name = package.split('/')[-1]
        my_rows = []
        for row in first:
            if row['package'] == package_name and row['source'] == 'packagist':
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
    yield packagist_collector(package, latest_row)


spew(datapackage, process_resources(res_iter, datapackage, package))

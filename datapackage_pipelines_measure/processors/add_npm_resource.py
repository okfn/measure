import datetime
import dateutil

import requests

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

# from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

NPM_REGISTRY_BASE_URL = "https://registry.npmjs.org/"
NPM_STATS_BASE_URL = "https://api.npmjs.org/downloads/point/"
NPM_STATS_DATE_RANGE_FORMAT = "%Y-%m-%d"
NPM_NO_DATA_ERROR_MSG = "no stats for this package for this period (0002)"


def _get_package_creation_date(package):
    '''Return for a given package the date at which it was created, from npm's
    registry api.

    :param package: the package name
    :return: a date object, representing the date of the given package's
        creation.
    '''
    response = requests.get(NPM_REGISTRY_BASE_URL.rstrip('/') + '/' + package)
    if not response.json():
        raise ValueError('Package "{}" returned no data from '
                         'npm registry. Check that the package '
                         'is published.'.format(package))
    time_created = response.json()['time']['created']
    return dateutil.parser.parse(time_created).date()


def _get_start_date(package, latest_date=None):
    '''Determine when data collection should start from.

    :param: package: the package name
    :latest_date: the most recent date data was collected for this package, if
        it exists
    '''
    creation_date = _get_package_creation_date(package)
    if latest_date:
        return max(latest_date, creation_date)
    else:
        return creation_date


def _get_requested_period_date_range(package, latest_date=None):
    '''Determine and return the required start_date and end_date for given
    package'''
    start_date = _get_start_date(package, latest_date)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    return start_date, end_date


def _add_metrics_collected_from_source(package, start_date_of_requested_period,
                                       end_date_of_requested_period):
    '''Add required metrics data from npm registry.

    The method executes multiple requests with different date frames,
    aggregates all the results, and returns them.
    '''

    start_date_of_window = start_date_of_requested_period

    collected_responses = []
    while start_date_of_window < end_date_of_requested_period:
        # Frame is one day.
        days_in_frame = 1
        end_date_of_window = start_date_of_window

        frame_response = _get_metrics_from_data_source(
            package,
            start_date=start_date_of_window.strftime(
                NPM_STATS_DATE_RANGE_FORMAT),
            end_date=end_date_of_window.strftime(NPM_STATS_DATE_RANGE_FORMAT)
        )

        collected_responses.append(frame_response)

        start_date_of_window = \
            start_date_of_window + datetime.timedelta(days=days_in_frame)

    return collected_responses


def _get_metrics_from_data_source(package, start_date, end_date):
    '''Get data from npm registry API for a given package, within a given date
    range.

    :param package: the package name
    :param start_date: start date of collection, as a correctly formatted
        string.
    :param end_date: end date of range, as a correctly formatted string.
    :return a json response.'''
    url_path = "{from_date}:{to_date}/{package}".format(
        from_date=start_date,
        to_date=end_date,
        package=package)
    response = requests.get(NPM_STATS_BASE_URL.rstrip('/') + '/' + url_path)
    if 'error' in response.json():
        if NPM_NO_DATA_ERROR_MSG in response.json()['error']:
            return {'package': package,
                    'start': start_date,
                    'end': end_date,
                    'downloads': 0}
        raise ValueError('package {} raised error:{}'
                         .format(package, response.json()['error']))
    return response.json()


def npm_collector(package, latest_date):
    start_date_of_requested_period, end_date_of_requested_period = \
        _get_requested_period_date_range(package, latest_date)

    collected_responses = \
        _add_metrics_collected_from_source(package,
                                           start_date_of_requested_period,
                                           end_date_of_requested_period)

    resource_content = []

    for response in collected_responses:
        row = {
            'package': package,
            'source': 'npm',
            'date': dateutil.parser.parse(response['start']).date(),
            'downloads': response['downloads']
        }
        resource_content.append(row)

    return resource_content


parameters, datapackage, res_iter = ingest()

package = parameters['package']
project_id = parameters['project_id']
resource = {
    'name': slugify(package),
    'path': 'data/{}.csv'.format(slugify(package))
}
# Get the basic resource schema from the first row
headers = ['package', 'source', 'date', 'downloads']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, package):

    def get_latest_date(first):
        latest_date = None
        my_rows = []
        for row in first:
            if row['package'] == package:
                latest_date = row['date']
            my_rows.append(row)
        return latest_date, iter(my_rows)

    if len(datapackage['resources']):
        log.debug('there is a resource')
        if datapackage['resources'][0]['name'] == 'latest-project-entries':
            log.debug('with the right name')
            latest_date, latest_iter = get_latest_date(next(res_iter))
            yield latest_iter
        else:
            latest_date = None
    yield from res_iter
    yield npm_collector(package, latest_date)


spew(datapackage, process_resources(res_iter, datapackage, package))

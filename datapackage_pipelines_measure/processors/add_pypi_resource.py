import datetime
import dateutil

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.processors import google_utils
from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

PYPI_DEFAULT_START_DATE = '2016-01-22'


def _request_data_from_bigquery(package, start_date, end_date):
    '''Build a google bigquery api service, then build a query and execute it.
    Return the results unless there's a error.
    '''
    def _build_bigquery_query(package):
        '''Using supplied parameters, the method renders the BigQuery Query for
        execution.

        :param: package: package name requested
        :returns: bigquery query as string, formatted with received packages'''

        query = '''
        SELECT
          file.project,
          STRFTIME_UTC_USEC(timestamp, "%Y-%m-%d") AS yyyymmdd,
          COUNT(*) AS total_downloads,
        FROM
          TABLE_DATE_RANGE([the-psf:pypi.downloads],
                           TIMESTAMP('{start_date}'),
                           TIMESTAMP('{end_date}'))
        WHERE
          file.project == '{package}'
        GROUP BY
          file.project,
          yyyymmdd
        ORDER BY
          yyyymmdd DESC
        '''.format(package=package,
                   start_date=start_date,
                   end_date=end_date)
        return query

    def _execute_bigquery_query_request(body, service):
        '''Execute a bigquery request.'''
        response = service.jobs().query(
            projectId=settings['GOOGLE_API_PROJECT_ID'],
            body=body).execute()
        if 'jobComplete' in response and not response['jobComplete']:
            raise Exception('Requested query did not complete '
                            'its execution yet.')
        if 'rows' not in response:
            raise ValueError('Data is missing in received response. '
                             'See it here:\n\n {}'.format(response))
        return response

    service = google_utils.get_google_api_service(
        google_utils.GOOGLE_API_BIGQUERY_SERVICE_NAME,
        google_utils.GOOGLE_API_BIGQUERY_VERSION,
        google_utils.GOOGLE_API_BIGQUERY_SCOPES
    )
    body = {
        "query": _build_bigquery_query(package), 'timeoutMs':
            google_utils.GOOGLE_API_BIGQUERY_DEFAULT_TIMEOUT_MILLISECONDS
    }
    return _execute_bigquery_query_request(body, service)


def _get_start_date(package, latest_date=None):
    '''Determine when data collection should start.

    :param: package: the package name
    :latest_date: the most recent date data was collected for this package, if
        it exists
    '''
    default_start = dateutil.parser.parse(PYPI_DEFAULT_START_DATE).date()
    if latest_date:
        return max(latest_date, default_start)
    else:
        return default_start


def _get_requested_period_date_range(package, latest_date=None):
    '''Determine and return the required start_date and end_date for given
    package'''
    start_date = _get_start_date(package, latest_date)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    return start_date, end_date


def pypi_collector(package, latest_date):
    start_date_of_requested_period, end_date_of_requested_period = \
        _get_requested_period_date_range(package, latest_date)

    api_response = _request_data_from_bigquery(package,
                                               start_date_of_requested_period,
                                               end_date_of_requested_period)

    resource_content = []
    for row in api_response['rows']:
        res_row = {
            'source': 'pypi',
            'package': row['f'][0]['v'],  # read: row, fields, column 0, value
            'date': dateutil.parser.parse(row['f'][1]['v']).date(),
            'downloads': int(row['f'][2]['v'])
        }
        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

package = parameters['package']
project_id = parameters['project_id']
resource = {
    'name': slugify(package),
    'path': 'data/{}.csv'.format(slugify(package))
}

headers = ['package', 'source', 'date', 'downloads']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, package):

    def get_latest_date(first):
        latest_date = None
        my_rows = []
        for row in first:
            if row['package'] == package and row['source'] == 'pypi':
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
    yield pypi_collector(package, latest_date)


spew(datapackage, process_resources(res_iter, datapackage, package))

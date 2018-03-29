import datetime
import dateutil

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.processors import google_utils

import logging
log = logging.getLogger(__name__)

GOOGLE_API_GA_TABLE_DATE_RANGE_FORMAT = '%Y-%m-%d'
GOOGLE_API_GA_TABLE_DEFAULT_FROM_DATE = '2005-01-01'


def _request_data_from_ga(domain, view_id, start_date, end_date):
    '''Build a google analytics api service, then build a query and execute it.
    Return the results unless there's a error.
    '''
    def _build_ga_query(view_id, start_date, end_date):
        start_date = start_date.strftime(GOOGLE_API_GA_TABLE_DATE_RANGE_FORMAT)
        end_date = end_date.strftime(GOOGLE_API_GA_TABLE_DATE_RANGE_FORMAT)
        query = {
            "reportRequests": [
                {
                    "viewId": view_id,
                    "dateRanges": [
                        {
                            "startDate": start_date,
                            "endDate": end_date
                        }
                    ],
                    "dimensions": [
                        {
                            "name": "ga:date"
                        },
                        {
                            "name": "ga:hostname"
                        },
                        {
                            "name": "ga:pagePath"
                        }
                    ],
                    "metrics": [
                        {
                            "expression": "ga:sessions"
                        },
                        {
                            "expression": "ga:users"
                        },
                        {
                            "expression": "ga:avgSessionDuration"
                        },
                        {
                            "expression": "ga:pageviews"
                        },
                    ],
                    "pageSize": "10000"
                }
            ]
        }
        return query

    service = google_utils.get_google_api_service(
        google_utils.GOOGLE_API_GA_SERVICE_NAME,
        google_utils.GOOGLE_API_GA_VERSION,
        google_utils.GOOGLE_API_GA_SCOPES
    )
    body = _build_ga_query(view_id, start_date, end_date)
    response = service.reports().batchGet(body=body).execute()

    if 'rows' in response['reports'][0]['data']:
        return [row for row in response['reports'][0]['data']['rows']]
    else:
        return []


def _get_start_date(latest_date=None):
    '''Determine when data collection should start.

    :latest_date: the most recent date data was collected for this domain, if
        it exists
    '''
    default_start = \
        dateutil.parser.parse(GOOGLE_API_GA_TABLE_DEFAULT_FROM_DATE).date()
    if latest_date:
        return max(latest_date, default_start)
    else:
        return default_start


def _get_requested_period_date_range(latest_date=None):
    '''Determine and return the required start_date and end_date'''
    start_date = _get_start_date(latest_date)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    return start_date, end_date


def ga_collector(domain, view_id, latest_date):
    start_date_of_requested_period, end_date_of_requested_period = \
        _get_requested_period_date_range(latest_date)

    api_response = _request_data_from_ga(domain, view_id,
                                         start_date_of_requested_period,
                                         end_date_of_requested_period)

    resource_content = []
    for row in api_response:
        metrics = row['metrics'][0]['values']
        res_row = {
            'source': 'ga',
            'domain': row['dimensions'][1],
            'date': dateutil.parser.parse(row['dimensions'][0]).date(),
            'page_path': row['dimensions'][2],
            'visitors': int(metrics[0]),
            'unique_visitors': int(metrics[1]),
            'avg_time_spent': round(float(metrics[2])),
            'pageviews': int(metrics[3]),
        }
        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

domain_url = parameters['domain']['url']
domain_view_id = parameters['domain']['viewid']
resource = {
    'name': slugify(domain_url),
    'path': 'data/{}.csv'.format(slugify(domain_url))
}

headers = ['domain', 'source', 'date', 'visitors', 'unique_visitors',
           'avg_time_spent', 'page_path', 'pageviews']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, domain, view_id):

    def get_latest_date(first):
        latest_date = None
        my_rows = []
        for row in first:
            if row['domain'] == domain and row['source'] == 'ga':
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
    yield ga_collector(domain, view_id, latest_date)


spew(datapackage, process_resources(res_iter, datapackage,
                                    domain_url, domain_view_id))

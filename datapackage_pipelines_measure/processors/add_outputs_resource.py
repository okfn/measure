import re
import json
import dateutil
import urllib

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.processors import google_utils

import logging
log = logging.getLogger(__name__)

TIMESTAMP_COL = 'A'
FAR_PAST_START_DATE = '1990-01-01'


def _request_data_from_google_spreadsheet(start_date):
    '''
    Build a google charts query and append it to an authorised spreadsheets
    request, returning the response.
    '''
    def _build_charts_query(start_date):
        '''
        Build and return a charts query to fetch the most recent rows from the
        spreadsheet, based on the most recent data collected for this source.
        '''
        query = '''
            SELECT *
            WHERE {timestamp} > date '{start_date}'
            ORDER BY {timestamp}
            '''.format(timestamp=TIMESTAMP_COL, start_date=start_date)
        query = query.strip()
        return urllib.parse.quote(query)

    def _parse_response_to_dict(response):
        '''Parse the response from google api and return the bit we want as a
        native dict'''
        regexp = re.compile(b"(^\/\*O_o\*\/\\ngoogle\.visualization\.Query\.setResponse\(|\);$)") # noqa
        return json.loads(re.sub(regexp, b'', response[1]).decode())

    base_request_url = 'https://docs.google.com/spreadsheets/d/{}/gviz/tq?gid={}&headers=1&tq={}' # noqa
    request_url = base_request_url.format(sheet_id, gid,
                                          _build_charts_query(start_date))

    authed_http = google_utils.get_authorized_http_object(
        google_utils.GOOGLE_API_DRIVE_SCOPES)

    raw_response = authed_http.request(request_url)

    response = _parse_response_to_dict(raw_response)
    if response.get('status') == 'error':
        raise ValueError('The following error was returned:\n{}'
                         .format(response['errors'][0].get('detailed_message'))) # noqa
    return response


def form_collector(source_id, source_type, latest_date):
    start_date = FAR_PAST_START_DATE
    if latest_date:
        start_date = latest_date.date()

    response = _request_data_from_google_spreadsheet(start_date)

    resource_content = []
    headers = response['table']['cols']
    headers = [slugify(h['label'].lower()) for h in headers]
    for r in response['table']['rows']:
        row = r['c']
        row_dict = {}
        for i, v in enumerate(row):
            if v is not None:
                row_dict[headers[i]] = v.get('f') or v.get('v')
            else:
                row_dict[headers[i]] = None
        output_date = dateutil.parser.parse(row_dict.get('date')).date() \
            if row_dict.get('date') is not None else None
        res_row = {
            'source_id': source_id,
            'source_type': source_type,
            'source': 'gsheets',
            'source_timestamp':
                dateutil.parser.parse(row_dict.get('timestamp')),
            'source_email': row_dict.get('email-address'),
            'output_title': row_dict.get('title'),
            'output_type': row_dict.get('type-of-output'),
            'output_organization': row_dict.get('for-what-organisation'),
            'output_person': row_dict.get('who-did-this'),
            'output_link': row_dict.get('link-if-published'),
            'output_additional_information':
                row_dict.get('additional-information'),
            'output_date': output_date
        }
        resource_content.append(res_row)

    return resource_content


def process_resources(res_iter, datapackage, source_id, source_type):

    def get_latest_date(first):
        latest_date = None
        my_rows = []
        for row in first:
            if row['source_id'] == source_id and row['source'] == 'gsheets':
                latest_date = row['source_timestamp']
            my_rows.append(row)
        return latest_date, iter(my_rows)

    if len(datapackage['resources']):
        if datapackage['resources'][0]['name'] == 'latest-project-entries':
            latest_date, latest_iter = get_latest_date(next(res_iter))
            yield latest_iter
        else:
            latest_date = None
    yield from res_iter
    yield form_collector(source_id, source_type, latest_date)


parameters, datapackage, res_iter = ingest()

sheet_id = parameters['sheet_id']
gid = parameters['gid']
source_type = parameters['source_type']
source_id = '{0}/{1}'.format(sheet_id, gid)
resource = {
    'name': slugify(sheet_id).lower(),
    'path': 'data/{}.csv'.format(slugify(sheet_id))
}

headers = ['source', 'source_type', 'source_timestamp', 'source_email',
           'output_title', 'output_type', 'output_organization',
           'output_person', 'output_link', 'output_additional_information',
           'output_date']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)

spew(datapackage, process_resources(res_iter, datapackage,
                                    source_id, source_type))

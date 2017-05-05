import datetime
import itertools
import dateutil

import facebook as facebook_sdk

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings
# from datapackage_pipelines_measure.datastore import get_datastore

import logging
log = logging.getLogger(__name__)


FACEBOOK_API_DATE_RANGE_FORMAT = '%Y-%m-%d'
FACEBOOK_API_VERSION = 'v2.9'

# dicts representing the connection between a model field and the respective
# metrics in the Facebook API.
model_to_metric = [
    {'model_field': 'impressions',
     'facebook_metric': 'page_impressions',
     'facebook_breakdown': None, 'period_is_lifetime': False},
    {'model_field': 'followers',
     'facebook_metric': 'page_fans',
     'facebook_breakdown': None, 'period_is_lifetime': True},
    {'model_field': 'interactions',
     'facebook_metric': 'page_stories',
     'facebook_breakdown': None, 'period_is_lifetime': False},
    {'model_field': 'mentions',
     'facebook_metric': 'page_stories_by_story_type',
     'facebook_breakdown': 'mention', 'period_is_lifetime': False},
]
graph = facebook_sdk.GraphAPI()


def _request_data_from_facebook_api(page, metrics, period, since, until):
    '''Execute a request from facebook api, and return the response. parameters
    are documented here:
    developers.facebook.com/docs/graph-api/reference/page/insights/
    '''
    def _get_page_access_token_from_config(page):
        '''Get the access token for the given page from the config'''
        token_name = 'FACEBOOK_API_ACCESS_TOKEN_' + page.upper()
        try:
            return getattr(settings, token_name)
        except AttributeError:
            raise RuntimeError('No Facebook Page Access Token found for '
                               'page: "{}" in settings'.format(page))

    graph.access_token = _get_page_access_token_from_config(page)
    path = '{version}/{page}/insights/'.format(version=FACEBOOK_API_VERSION,
                                               page=page)
    args = {
        'metric': ','.join([metric['facebook_metric'] for metric in
                            metrics]),
        'period': period,
        'since': since,
        'until': until
    }
    try:
        response = graph.request(path=path, args=args)
    except facebook_sdk.GraphAPIError as e:
        raise e
    if not response['data']:
        raise ValueError('Facebook request returned no data.')
    return response


def _get_lifetime_metrics_from_source(page):
    '''Get metrics that are available on Facebook Insights API as so-called
    "Lifetime" period, i.e cumulative, and return them with no further
    calculations.

    Get lifetime metrics as they were yesterday (the last whole day).
    '''
    def _get_metric_value_by_date(metric_values, requested_date):
        '''From a list of values for a given metric, return the value of the
        requested date. If date does not exist in list, raise error.
        '''
        for value in metric_values:
            date_of_value = dateutil.parser.parse(value['end_time']).date()
            if date_of_value == requested_date:
                return value['value']
        raise ValueError('requested date:"{}" was not found in '
                         'returned values.'.format(requested_date))

    def _flatten_collected_metric(collected_metric, requested_date):
        '''Return a two-tuple of metric name, and value of metric at requested
        date.'''
        return collected_metric['name'], _get_metric_value_by_date(
            collected_metric['values'], requested_date)

    def _flatten_lifetime_metrics_facebook_response(requested_date,
                                                    response):
        '''Take a facebook response with cumulative data, and return it as a
        dictionary with metric_name:metric_value items.
        '''
        return dict(
            _flatten_collected_metric(collected_metric, requested_date)
            for collected_metric in response['data']
        )

    lifetime_metrics = [metric for metric in model_to_metric if
                        metric['period_is_lifetime']]

    end_date = datetime.date.today()  # end_date is exclusive (so yesteray)
    start_date = end_date - datetime.timedelta(days=1)

    response = _request_data_from_facebook_api(
        page=page,
        metrics=lifetime_metrics,
        period='lifetime',
        since=start_date.strftime(FACEBOOK_API_DATE_RANGE_FORMAT),
        until=end_date.strftime(FACEBOOK_API_DATE_RANGE_FORMAT)
    )

    return _flatten_lifetime_metrics_facebook_response(start_date, response)


parameters, datapackage, res_iter = ingest()

entity = parameters['entity']
safe_entity = slugify(entity).lower()
resource = {
    'name': safe_entity,
    'path': 'data/{}.json'.format(safe_entity)
}
entity_type = 'page'

resource_content = {
    'entity': entity,
    'entity_type': entity_type,
    'source': 'facebook'
}

followers_count = None
lifetime_metrics = _get_lifetime_metrics_from_source(entity)
resource_content.update({
    'followers': lifetime_metrics['page_fans']
})

resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in resource_content.keys()]}

datapackage['resources'].append(resource)

# Make a single-item generator from the resource_content
resource_content = itertools.chain([resource_content])

spew(datapackage, itertools.chain(res_iter, [resource_content]))

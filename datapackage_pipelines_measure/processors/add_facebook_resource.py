import datetime
import itertools
import dateutil
import collections

import facebook as facebook_sdk

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings
from datapackage_pipelines_measure.datastore import get_datastore

import logging
log = logging.getLogger(__name__)

FACEBOOK_API_VERSION = 'v2.9'
FACEBOOK_API_DEFAULT_START_DATE = '2014-09-02'
FACEBOOK_API_DATE_RANGE_FORMAT = '%Y-%m-%d'
FACEBOOK_API_MAX_DAYS_IN_RANGE = 92
DATASTORE_TABLE = 'socialmedia'

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


def _get_daily_metrics_from_source(page, start_date, end_date):
    '''Get metrics from Facebook API that are not held in a cumulative format.

    The method executes multiple requests, each for up to the maximal allowed
    date range, and aggregates the results.
    '''
    def _get_number_of_days_in_frame(start_date_of_window,
                                     end_date_of_requested_period):
        '''Get the number of days in frame, as the total number of days left,
        or the max number of days in ranger, whichever is lower.
        '''
        return min(
            (end_date_of_requested_period - start_date_of_window).days,
            FACEBOOK_API_MAX_DAYS_IN_RANGE
        )

    def _add_collected_metric_to_aggregation(collected_response, metric):
        '''Add the returned values from a given facebook request, to the
        aggregated data.
        '''
        def _get_a_collected_metric_value(daily_value, metric):
            '''Return the value of a given metric. If a metric value is a
            breakdown dictionary, return the value by the breakdown name.
            Otherwise, return the value as is.
            '''
            if metric['facebook_breakdown']:
                return daily_value['value'][metric['facebook_breakdown']]
            else:
                return daily_value['value']

        aggregated_value = 0
        try:
            collected_values_for_metric = \
                [collected for collected in collected_response['data'] if
                 collected['name'] == metric['facebook_metric']][0]
        except IndexError:
            raise ValueError('Metric, "{}", was not found in found in '
                             'returned data.'.format(
                                metric['facebook_metric']))

        for daily_value in collected_values_for_metric['values']:
            aggregated_value += _get_a_collected_metric_value(daily_value,
                                                              metric)

        return aggregated_value

    daily_metrics = [metric for metric in model_to_metric if
                     not metric['period_is_lifetime']]
    aggregated_metrics = collections.defaultdict(int)

    start_date_frame = start_date
    while start_date_frame + datetime.timedelta(days=1) < end_date:
        days_in_frame = _get_number_of_days_in_frame(start_date_frame,
                                                     end_date)
        end_date_frame = start_date_frame + datetime.timedelta(
            days=days_in_frame)

        frame_response = _request_data_from_facebook_api(
            page=page,
            metrics=daily_metrics,
            period='day',
            since=start_date_frame.strftime(FACEBOOK_API_DATE_RANGE_FORMAT),
            until=end_date_frame.strftime(FACEBOOK_API_DATE_RANGE_FORMAT)
        )
        for metric in daily_metrics:
            aggregated_metrics[metric['facebook_metric']] += \
                _add_collected_metric_to_aggregation(frame_response, metric)
        start_date_frame = end_date_frame

    return aggregated_metrics


def _get_default_start_date():
    '''Get the default start date, first try from the config.settings, then
    fallback to the module variable. This enables tests to override the value.
    '''
    try:
        return settings.FACEBOOK_API_DEFAULT_START_DATE
    except AttributeError:
        return FACEBOOK_API_DEFAULT_START_DATE


parameters, datapackage, res_iter = ingest()

project_id = parameters['project_id']
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

lifetime_metrics = _get_lifetime_metrics_from_source(entity)
resource_content.update({
    'followers': lifetime_metrics['page_fans']
})

# Get last run entity values
filter_object = {
    'entity': entity,
    'entity_type': entity_type,
    'source': 'facebook',
    'project_id': project_id
}
datastore = get_datastore()
entity_last_run = datastore.get_latest_from_table(filter_object,
                                                  DATASTORE_TABLE)

mentions = 0
interactions = 0
impressions = 0
if entity_last_run:
    latest_mentions = entity_last_run.get('mentions', 0)
    latest_interactions = entity_last_run.get('interactions', 0)
    latest_impressions = entity_last_run.get('impressions', 0)
    if entity_last_run['timestamp'].date() == datetime.date.today():
        log.debug('Last run for entity, "{}", was today.'.format(entity))
        # Last run today, so don't collect, use data from entity_last_run.
        mentions = latest_mentions
        interactions = latest_interactions
        impressions = latest_impressions
    elif entity_last_run['timestamp'].date() < datetime.date.today():
        # Last run before today, get data from the last run date to yesterday.
        log.debug('Last run for entity, "{}", was before today.'
                  .format(entity))
        end_date = datetime.date.today()  # end_date is exclusive (so yesteray)
        start_date = entity_last_run['timestamp'].date()
        daily_metrics = _get_daily_metrics_from_source(entity, start_date,
                                                       end_date)
        interactions = latest_interactions + daily_metrics['page_stories']
        impressions = latest_impressions + daily_metrics['page_impressions']
        mentions = \
            latest_mentions + daily_metrics['page_stories_by_story_type']
else:
    # No last run, this is the first entry for this entity.
    # Collect historic facebook data for entity from the default start date.
    end_date = datetime.date.today()  # end_date is exclusive (so yesterday)
    start_date = dateutil.parser.parse(_get_default_start_date()).date()
    daily_metrics = _get_daily_metrics_from_source(entity, start_date,
                                                   end_date)
    interactions = daily_metrics['page_stories']
    impressions = daily_metrics['page_impressions']
    mentions = daily_metrics['page_stories_by_story_type']

resource_content.update({
    'mentions': mentions,
    'interactions': interactions,
    'impressions': impressions
})

resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in resource_content.keys()]}

datapackage['resources'].append(resource)

# Make a single-item generator from the resource_content
resource_content = itertools.chain([resource_content])

spew(datapackage, itertools.chain(res_iter, [resource_content]))

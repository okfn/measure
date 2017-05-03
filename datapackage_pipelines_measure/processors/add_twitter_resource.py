import datetime
import itertools
import time
from functools import lru_cache

import tweepy

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings
from datapackage_pipelines_measure.datastore import get_datastore

import logging
log = logging.getLogger(__name__)

tweepy_cursor = tweepy.Cursor

ENTITY_VALUE_ERROR_MSG = 'Entity, "{}", must be an @account or a #hashtag'
TWITTER_API_USER_NOT_FOUND_ERROR_CODE = "50"
TWITTER_API_DATE_RANGE_FORMAT = '%Y-%m-%d'
TWITTER_API_PER_PAGE_LIMIT = 100
TWITTER_API_SEARCH_INDEX_LIMIT_IN_DAYS = 3
TWITTER_API_RATE_LIMIT_PERIOD = 900  # 15 mins
DATASTORE_TABLE = 'socialmedia'


def _get_entity_type(entity):
    '''Get the entity type, based on the starting character.'''
    if entity.startswith('@'):
        return 'account'
    elif entity.startswith('#'):
        return 'hashtag'
    else:
        raise ValueError(ENTITY_VALUE_ERROR_MSG.format(entity))


def _get_safe_entity(entity):
    '''Get a url safe version of the entity, base on starting character.'''
    if entity.startswith('@'):
        return 'at-{}'.format(slugify(entity))
    elif entity.startswith('#'):
        return 'hash-{}'.format(slugify(entity))
    else:
        raise ValueError(ENTITY_VALUE_ERROR_MSG.format(entity))


def _get_twitter_api_handler():
    '''Initialize a twitter API handler with an App access token'''
    auth = tweepy.auth.AppAuthHandler(settings.TWITTER_API_CONSUMER_KEY,
                                      settings.TWITTER_API_CONSUMER_SECRET)
    return tweepy.API(auth)


def _get_user_account_from_twitter_api(entity):
    '''Get a user account from twitter api, and raise an informative error
    message if the user is not found'''
    api = _get_twitter_api_handler()
    try:
        return api.get_user(entity)
    except tweepy.TweepError as e:
        if str(e.api_code) == TWITTER_API_USER_NOT_FOUND_ERROR_CODE:
            raise ValueError('User with name, "{}", was not found. '
                             'Check your configuration'.format(entity))
        raise e


def _handle_twitter_rate_limit(cursor):
    '''Handle twitter rate limits. If rate limit is reached, the next element
    will be accessed again after sleep time'''
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            log.info('Twitter API rate limit error. Sleeping for {} secs.') \
                .format(TWITTER_API_RATE_LIMIT_PERIOD)
            sleep_time = TWITTER_API_RATE_LIMIT_PERIOD
            time.sleep(sleep_time)
        except tweepy.TweepError as e:
            if str(e.api_code) == TWITTER_API_USER_NOT_FOUND_ERROR_CODE:
                raise ValueError(
                    'Requested user was not found. Check your configuration')
            raise e


@lru_cache()
def _get_twitter_search_results(entity, formatted_start_date,
                                formatted_end_date):
    '''This method calls for search in twitter's API, and returns the list of
    tweets that matched the search within given time frame, including retweets.

    :param entity: the searched term.
    :param formatted_start_date: the starting date of period (inclusive).
    :param formatted_end_date: the end date of period (exclusive).
    :return a list of tweets'''

    query_args = {
        'q': entity, 'count': TWITTER_API_PER_PAGE_LIMIT,
        'result_type': 'recent', 'include_entities': False,
        'since': formatted_start_date, 'until': formatted_end_date
    }
    all_tweets_in_search = []
    api = _get_twitter_api_handler()

    for tweet in _handle_twitter_rate_limit(tweepy_cursor(api.search,
                                                          **query_args)
                                            .items()):
        all_tweets_in_search.append(tweet)
    return all_tweets_in_search


def _get_account_tweets(entity, start_date, end_date):
    '''This method iterates over the tweets written by the given user, and
    returns a list of tweets in given time period.

    Since there's no built-in option for time filtering, it is done by
    iterating from latest to oldest, excluding too-new, and stopping once too-
    old tweets are reached.

    :param entity: the user who's timeline is iterated over.
    :param start_date: the earliest date of the period (inclusive).
    :param end_date: the end date of the period (exclusive).
    '''
    all_self_tweets_in_time_period = []
    user_tweets_args = {'screen_name': entity, 'count': 200,
                        'include_rts': False, 'exclude_replies': False}
    start_date = datetime. \
        datetime.strptime(start_date, TWITTER_API_DATE_RANGE_FORMAT).date()
    end_date = datetime. \
        datetime.strptime(end_date, TWITTER_API_DATE_RANGE_FORMAT).date()
    api = _get_twitter_api_handler()

    for tweet in _handle_twitter_rate_limit(tweepy_cursor(
            api.user_timeline, **user_tweets_args).items()):
        if tweet.created_at.date() >= end_date:
            continue
        if tweet.created_at.date() < start_date:
            break
        all_self_tweets_in_time_period.append(tweet)
    return all_self_tweets_in_time_period


def _get_mentions_for_entity_from_source(entity, start_date, end_date):
    '''Get the mentions metric for entity type.'''
    all_tweets_in_search = _get_twitter_search_results(entity,
                                                       start_date,
                                                       end_date)
    return len(all_tweets_in_search)


def _get_interactions_for_entity_from_source(entity, start_date, end_date):
    def _count_indicators_of_interaction(list_of_tweets):
        '''This methods iterates over a list of tweets, and returns a two-tuple
        of total count of favorites, and total count of retweets. The method
        makes sure it doesn't double-count stats, so it skips tweets that are
        themselves retweets, and counts only original tweets'''
        favorite_count, retweet_count = 0, 0
        for tweet in list_of_tweets:
            if getattr(tweet, 'retweeted_status', None):
                continue
            favorite_count += tweet.favorite_count
            retweet_count += tweet.retweet_count
        return favorite_count, retweet_count

    entity_type = _get_entity_type(entity)
    if entity_type == 'account':
        '''
        Get interaction for a twitter account entity. This is done by getting
        all the account tweets during given date range, and aggregate the
        measured 'interaction' metrics - favorites and retweets.
        '''
        tweets = _get_account_tweets(entity, start_date, end_date)
    elif entity_type == 'hashtag':
        '''
        Get interactions for a hashtag entity. This is done by searching for
        the hashtag between the given date range, and aggregate the measured
        'interaction' metrics - favorites and retweets.
        '''
        tweets = _get_twitter_search_results(entity, start_date, end_date)
    favorite_count, retweet_count = _count_indicators_of_interaction(tweets)
    return sum([favorite_count, retweet_count])


parameters, datapackage, res_iter = ingest()

entity = parameters['entity']
project_id = parameters['project_id']
safe_entity = _get_safe_entity(entity)
resource = {
    'name': safe_entity,
    'path': 'data/{}.json'.format(safe_entity)
}

entity_type = _get_entity_type(entity)

# Followers
# This is requested directly from the twitter api

followers_count = None
if entity_type == 'account':
    user = _get_user_account_from_twitter_api(entity)
    followers_count = user.followers_count


# Mentions & Interactions
# These are requested for specified (and limited) timeframes from the twitter
# api, then added to the previous result to get a total since we started
# collecting data for the entity.

# Get last run entity values
filter_object = {
    'entity': entity,
    'entity_type': entity_type,
    'source': 'twitter',
    'project_id': project_id
}
datastore = get_datastore()
entity_last_run = datastore.get_latest_from_table(filter_object,
                                                  DATASTORE_TABLE)
mentions = 0
interactions = 0
if entity_last_run:
    latest_mentions = entity_last_run.get('mentions', 0)
    latest_interactions = entity_last_run.get('interactions', 0)
    if entity_last_run['timestamp'].date() == datetime.date.today():
        # Last run today
        log.debug('Last run for entity, "{}". was today.'.format(entity))
        # Don't collect from twitter again, just reuse entity_last_run's data
        mentions = latest_mentions
        interactions = latest_interactions
    elif entity_last_run['timestamp'].date() < datetime.date.today():
        # Last run before today, get tweets from the last run date to
        # yesterday.
        log.debug('Last run for entity, "{}", was before today.'
                  .format(entity))
        end_date = datetime.date.today()  # end_date is exclusive (so yesteray)
        start_date = entity_last_run['timestamp'].date()
        mentions = _get_mentions_for_entity_from_source(
            entity,
            start_date.strftime(TWITTER_API_DATE_RANGE_FORMAT),
            end_date.strftime(TWITTER_API_DATE_RANGE_FORMAT)
        )
        mentions = mentions + latest_mentions
        interactions = _get_interactions_for_entity_from_source(
            entity,
            start_date.strftime(TWITTER_API_DATE_RANGE_FORMAT),
            end_date.strftime(TWITTER_API_DATE_RANGE_FORMAT)
        )
        interactions = interactions + latest_interactions
else:
    # No last run for this entity.
    log.debug('No last run for entity "{}"'.format(entity))
    # Collect twitter data for entity for the last few days
    end_date = datetime.date.today()  # end_date is exclusive (so yesterday)
    start_date = (end_date - datetime.timedelta(
        days=TWITTER_API_SEARCH_INDEX_LIMIT_IN_DAYS))
    mentions = _get_mentions_for_entity_from_source(
        entity,
        start_date.strftime(TWITTER_API_DATE_RANGE_FORMAT),
        end_date.strftime(TWITTER_API_DATE_RANGE_FORMAT)
    )
    interactions = _get_interactions_for_entity_from_source(
        entity,
        start_date.strftime(TWITTER_API_DATE_RANGE_FORMAT),
        end_date.strftime(TWITTER_API_DATE_RANGE_FORMAT)
    )

resource_content = {
    'entity': entity,
    'entity_type': entity_type,
    'source': 'twitter',
    'followers': followers_count,
    'mentions': mentions,
    'interactions': interactions
}

resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in resource_content.keys()]}

datapackage['resources'].append(resource)

# Make a single-item generator from the resource_content
resource_content = itertools.chain([resource_content])

spew(datapackage, itertools.chain(res_iter, [resource_content]))

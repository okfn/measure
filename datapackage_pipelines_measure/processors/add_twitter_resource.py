import itertools

import tweepy

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

ENTITY_VALUE_ERROR_MSG = 'Entity, "{}", must be an @account or a #hashtag'
TWITTER_API_USER_NOT_FOUND_ERROR_CODE = "50"


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


parameters, datapackage, res_iter = ingest()

entity = parameters['entity']
safe_entity = _get_safe_entity(entity)
resource = {
    'name': safe_entity,
    'path': 'data/{}.json'.format(safe_entity)
}

entity_type = _get_entity_type(entity)
followers_count = None
if entity_type == 'account':
    user = _get_user_account_from_twitter_api(entity)
    followers_count = user.followers_count
resource_content = {
    'entity': entity,
    'entity_type': entity_type,
    'source': 'twitter',
    'followers': followers_count
}


resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in resource_content.keys()]}

datapackage['resources'].append(resource)

# Make a single-item generator from the resource_content
resource_content = itertools.chain([resource_content])

spew(datapackage, itertools.chain(res_iter, [resource_content]))

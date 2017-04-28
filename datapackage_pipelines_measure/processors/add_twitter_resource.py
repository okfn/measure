import itertools

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

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


parameters, datapackage, res_iter = ingest()

entity = parameters['entity']
safe_entity = _get_safe_entity(entity)
resource = {
    'name': safe_entity,
    'path': 'data/{}.json'.format(safe_entity)
}
resource_content = {
    'entity': entity,
    'entity_type': _get_entity_type(entity),
    'source': 'twitter'
}


resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in resource_content.keys()]}

datapackage['resources'].append(resource)

# Make a single-item generator from the resource_content
resource_content = itertools.chain([resource_content])

spew(datapackage, itertools.chain(res_iter, [resource_content]))

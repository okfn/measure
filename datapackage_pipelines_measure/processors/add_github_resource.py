import json
import itertools

import requests
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()

name = str(parameters['name'])
repo = parameters.get('repo')
repo_url = '{}{}?access_token={}'.format(settings.GITHUB_API_BASE_URL,
                                         repo,
                                         settings.GITHUB_API_TOKEN)

try:
    repo_content = requests.get(repo_url).json()
except json.decoder.JSONDecodeError:
    log.error('Expected JSON in response from: {}'.format(repo_url))
    raise

# remap retrieved dict to scheme in parameters
resource_content = {t_key: repo_content[s_key]
                    for t_key, s_key in parameters['map_fields'].items()}
resource_content['source'] = 'github'

resource = {
    'name': name,
    'path': 'data/{}.json'.format(name)
}

# Temporarily set all types to string, will use `set_types` processor in
# pipeline to assign correct types
resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in resource_content.keys()]}

datapackage['resources'].append(resource)

# Make a single-item generator from the resource_content
resource_content = itertools.chain([resource_content])

spew(datapackage, itertools.chain(res_iter, [resource_content]))

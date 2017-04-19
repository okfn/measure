import collections
import tempfile
import json
import itertools

import requests
import tabulator
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

resource_content = collections.OrderedDict(sorted(resource_content.items(),
                                                  key=lambda t: t[0]))

resource = {
    'name': name,
    'path': 'data/{}.json'.format(name)
}

with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as out:
    out.write(bytes(json.dumps([resource_content]), encoding='utf-8'))

headers = list(resource_content.keys())

with tabulator.Stream('file://'+out.name,
                      format='json', headers=headers) as stream:
    # temporarily set all types to string, will use `set_types` processor in
    # pipeline to assign correct types
    resource['schema'] = {
        'fields': [{'name': h, 'type': 'string'} for h in stream.headers]}

datapackage['resources'].append(resource)

spew(datapackage, itertools.chain(res_iter, [stream.iter(keyed=True)]))

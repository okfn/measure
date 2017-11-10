import json
import datetime
import itertools

import requests
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()


def _make_github_request(url):
    try:
        return requests.get(url).json()
    except json.decoder.JSONDecodeError:
        log.error('Expected JSON in response from: {}'.format(url))
        raise


def _get_issue_count_for_request(url):
    issue_json = _make_github_request(url)
    return issue_json['total_count']


name = str(parameters['name'])
repo = parameters.get('repo')
base_url = settings['GITHUB_API_BASE_URL'].rstrip('/')

# BASE REPO INFO
base_repo_url = '{}/repos/{}?access_token={}'.format(
    base_url, repo, settings['GITHUB_API_TOKEN'])
# resource schema to api property names
map_fields = {
    'repository': 'name',
    'watchers': 'subscribers_count',
    'stars': 'stargazers_count'
}

repo_content = _make_github_request(base_repo_url)

resource_content = []
row = {t_key: repo_content[s_key] for t_key, s_key in map_fields.items()}
row['source'] = 'github'
row['date'] = datetime.date.today()

resource = {
    'name': name,
    'path': 'data/{}.csv'.format(name)
}

# Temporarily set all types to string, will use `set_types` processor in
# pipeline to assign correct types
resource['schema'] = {
    'fields': [{'name': h, 'type': 'string'} for h in row.keys()]}

datapackage['resources'].append(resource)

resource_content.append(row)

spew(datapackage, itertools.chain(res_iter, [resource_content]))

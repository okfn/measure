import simplejson
import datetime
import itertools
import time

import requests
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()

# 30 authenticated requests per minute, so wait 3 secs (or use
# GITHUB_REQUEST_WAIT_INTERVAL env var) before each request
# (https://developer.github.com/v3/search/#rate-limit)
REQUEST_WAIT_INTERVAL = int(settings.get('GITHUB_REQUEST_WAIT_INTERVAL', 3))


def _make_github_request(url):
    try:
        headers = {
            'Authorization': 'token {}'.format(settings['GITHUB_API_TOKEN'])
        }
        response = requests.get(url, headers=headers)
        json_response = response.json()
    except simplejson.scanner.JSONDecodeError:
        log.error('Expected JSON in response from: {}'.format(url))
        raise

    if response.status_code != 200:
        log.error('Response from Github not successful')
        raise RuntimeError(json_response)

    return json_response


def _get_issue_count_for_request(url):
    time.sleep(REQUEST_WAIT_INTERVAL)
    issue_json = _make_github_request(url)
    return issue_json['total_count']


name = str(parameters['name'])
repo = parameters.get('repo')
base_url = settings['GITHUB_API_BASE_URL'].rstrip('/')

# BASE REPO INFO
base_repo_url = '{}/repos/{}'.format(base_url, repo)
# resource schema to api property names
map_fields = {
    'repository': 'name',
    'watchers': 'subscribers_count',
    'stars': 'stargazers_count'
}

repo_content = _make_github_request(base_repo_url)
# Search queries require the current full name, rather than old names that
# redirect
current_repo_name = repo_content['full_name']

resource_content = []
row = {t_key: repo_content[s_key] for t_key, s_key in map_fields.items()}
row['source'] = 'github'
row['date'] = datetime.date.today()

# ISSUES & PR COUNTS
base_issue_url = '{}/search/issues?q=repo:{}'.format(
    base_url, current_repo_name)

row['open_prs'] = _get_issue_count_for_request(
    '{}%20state:open%20is:pr'.format(base_issue_url))
row['closed_prs'] = _get_issue_count_for_request(
    '{}%20state:closed%20is:pr'.format(base_issue_url))
row['open_issues'] = _get_issue_count_for_request(
    '{}%20state:open%20is:issue'.format(base_issue_url))
row['closed_issues'] = _get_issue_count_for_request(
    '{}%20state:closed%20is:issue'.format(base_issue_url))

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

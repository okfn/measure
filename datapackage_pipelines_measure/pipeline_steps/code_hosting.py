import os

from datapackage_pipelines.generators import slugify

from datapackage_pipelines_measure.config import settings

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'code-hosting'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:
    for repo in config['github']['repositories']:
        steps.append(('measure.add_github_resource', {
            'name': slugify(repo),
            'repo': repo
        }))

    steps.append(('concatenate', {
        'sources':
            [slugify(repo) for repo in config['github']['repositories']],
        'target': {
            'name': 'code-hosting',
            'path': 'data/code-hosting.json'},
        'fields': {
            'repository': [],
            'watchers': [],
            'stars': [],
            'open_prs': [],
            'open_issues': [],
            'closed_prs': [],
            'closed_issues': [],
            'source': [],
            'date': []}
    }))

    steps.append(('set_types', {
        'types': {
            'repository': {
                'type': 'string',
            },
            'watchers': {
                'type': 'integer'
            },
            'stars': {
                'type': 'integer'
            },
            'open_prs': {
                'type': 'integer'
            },
            'open_issues': {
                'type': 'integer'
            },
            'closed_prs': {
                'type': 'integer'
            },
            'closed_issues': {
                'type': 'integer'
            },
            'date': {
                'type': 'date',
            },
        }
    }))

    steps.append(('measure.add_project_name', {'name': project_id}))
    steps.append(('measure.add_timestamp'))
    steps.append(('measure.add_uuid'))

    # Dump to path if in development mode
    if settings.get('DEVELOPMENT', False):
        steps.append(('dump.to_path', {
            'out-path': '{}/{}'.format(DOWNLOADS_PATH, pipeline_id)
        }))

    steps.append(('dump.to_sql', {
        'engine': settings['DB_ENGINE'],
        'tables': {
            'codehosting': {
                'resource-name': 'code-hosting',
                'mode': 'update',
                'update_keys': ['repository', 'source', 'project_id', 'date']
            }
        }
    }))

    return steps

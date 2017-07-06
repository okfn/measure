import os

from datapackage_pipelines.generators import slugify

from datapackage_pipelines_measure.config import settings

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'code-packaging'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:

    steps.append(('measure.datastore_get_latest', {
        'resource-name': 'latest-project-entries',
        'table': 'codepackaging',
        'engine': settings.get('DB_ENGINE'),
        'distinct_on': ['project_id', 'package', 'source']
    }))

    if 'npm' in config:
        for package in config['npm']['packages']:
            steps.append(('measure.add_npm_resource', {
                'package': slugify(package)
            }))

    if 'pypi' in config:
        for package in config['pypi']['packages']:
            steps.append(('measure.add_pypi_resource', {
                'package': slugify(package)
            }))

    if 'rubygems' in config:
        for gem in config['rubygems']['gems']:
            steps.append(('measure.add_rubygems_resource', {
                'gem_id': gem
            }))

    steps.append(('measure.remove_resource', {
        'name': 'latest-project-entries'
    }))

    steps.append(('concatenate', {
        'target': {
            'name': 'code-packaging',
            'path': 'data/code-packaging.csv'},
        'fields': {
            'date': [],
            'downloads': [],
            'total_downloads': [],
            'source': [],
            'package': []}
    }))

    steps.append(('set_types', {
        'types': {
            'downloads': {
                'type': 'integer'
            },
            'total_downloads': {
                'type': 'integer'
            },
            'source': {
                'type': 'string'
            },
            'date': {
                'type': 'date'
            },
            'package': {
                'type': 'string'
            }}
    }))

    steps.append(('measure.add_project_name', {
        'name': project_id
    }))
    steps.append(('measure.add_timestamp'))
    steps.append(('measure.add_uuid'))

    # Dump to path if in development mode
    if settings.get('DEVELOPMENT', False):
        steps.append(('dump.to_path', {
            'out-path': '{}/{}'.format(DOWNLOADS_PATH, pipeline_id)
        }))

    steps.append(('dump.to_sql', {
        'engine': settings.get('DB_ENGINE'),
        'tables': {
            'codepackaging': {
                'resource-name': 'code-packaging',
                'mode': 'update',
                'update_keys': ['project_id', 'date', 'package', 'source']
            }
        }
    }))

    return steps

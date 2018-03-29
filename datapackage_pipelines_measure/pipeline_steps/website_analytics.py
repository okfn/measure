import os

from datapackage_pipelines_measure.config import settings

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'website-analytics'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:

    steps.append(('measure.datastore_get_latest', {
        'resource-name': 'latest-project-entries',
        'table': 'websiteanalytics',
        'engine': settings.get('DB_ENGINE'),
        'distinct_on': ['project_id', 'domain', 'page_path', 'source']
    }))

    if 'ga' in config:
        for domain in config['ga']['domains']:
            steps.append(('measure.add_ga_resource', {
                'domain': domain
            }))

    steps.append(('measure.remove_resource', {
        'name': 'latest-project-entries'
    }))

    steps.append(('concatenate', {
        'target': {
            'name': 'website-analytics',
            'path': 'data/website-analytics.json'},
        'fields': {
            'domain': [],
            'page_path': [],
            'visitors': [],
            'unique_visitors': [],
            'avg_time_spent': [],
            'source': [],
            'date': [],
            'pageviews': [],
        }
    }))

    steps.append(('set_types', {
        'types': {
            'domain': {
                'type': 'string',
            },
            'page_path': {
                'type': 'string',
            },
            'visitors': {
                'type': 'integer'
            },
            'unique_visitors': {
                'type': 'integer'
            },
            'avg_time_spent': {
                'type': 'number'
            },
            'date': {
                'type': 'date',
            },
            'pageviews': {
                'type': 'integer',
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
            'websiteanalytics': {
                'resource-name': 'website-analytics',
                'mode': 'update',
                'update_keys': [
                    'domain',
                    'page_path',
                    'source',
                    'project_id',
                    'date',
                ]
            }
        }
    }))

    return steps

import os

from datapackage_pipelines_measure.config import settings

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'forums'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:

    steps.append(('measure.datastore_get_latest', {
        'resource-name': 'latest-project-entries',
        'table': 'forums',
        'engine': settings.get('DB_ENGINE'),
        'distinct_on': ['project_id', 'domain', 'source']
    }))

    if 'discourse' in config:
        for domain in config['discourse']['domains']:
            steps.append(('measure.add_discourse_resource', {
                'domain': domain
            }))

    steps.append(('measure.remove_resource', {
        'name': 'latest-project-entries'
    }))

    steps.append(('concatenate', {
        'target': {
            'name': 'forums',
            'path': 'data/forums.json'},
        'fields': {
            'domain': [],
            'new_users': [],
            'new_topics': [],
            'new_posts': [],
            'visits': [],
            'active_users': [],
            'source': [],
            'date': []}
    }))

    steps.append(('set_types', {
        'types': {
            'domain': {
                'type': 'string',
            },
            'source': {
                'type': 'string',
            },
            'new_users': {
                'type': 'integer'
            },
            'new_topics': {
                'type': 'integer'
            },
            'new_posts': {
                'type': 'integer'
            },
            'visits': {
                'type': 'integer'
            },
            'active_users': {
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
            'forums': {
                'resource-name': 'forums',
                'mode': 'update',
                'update_keys': ['domain', 'source', 'project_id', 'date']
            }
        }
    }))

    return steps

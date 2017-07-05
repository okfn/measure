import os

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'email'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:

    steps.append(('measure.datastore_get_latest', {
        'resource-name': 'latest-project-entries',
        'table': 'email',
        'engine': settings.get('DB_ENGINE'),
        'distinct_on': ['project_id', 'source', 'list_id']
    }))

    if 'mailchimp' in config:
        for list_id in config['mailchimp']['lists']:
            steps.append(('measure.add_mailchimp_resource', {
                'list_id': list_id
            }))

    steps.append(('measure.remove_resource', {
        'name': 'latest-project-entries'
    }))

    steps.append(('concatenate', {
        'target': {
            'name': 'email',
            'path': 'data/email.csv'},
        'fields': {
            'source': [],
            'list_id': [],
            'date': [],
            'subscribers': [],
            'subs': [],
            'unsubs': [],
            'campaigns_sent': []
        }
    }))

    steps.append(('set_types', {
        'types': {
            'source': {
                'type': 'string'
            },
            'list_id': {
                'type': 'string'
            },
            'date': {
                'type': 'date'
            },
            'subscribers': {
                'type': 'integer'
            },
            'subs': {
                'type': 'integer'
            },
            'unsubs': {
                'type': 'integer'
            },
            'campaigns_sent': {
                'type': 'integer'
            }
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
        'engine': settings.get('DB_ENGINE'),
        'tables': {
            'email': {
                'resource-name': 'email',
                'mode': 'update',
                'update_keys': ['date', 'source', 'list_id', 'project_id']
            }
        }
    }))

    return steps

import os

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'outputs'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:

    steps.append(('measure.datastore_get_latest', {
        'resource-name': 'latest-project-entries',
        'table': 'outputs',
        'engine': settings.get('DB_ENGINE'),
        'distinct_on': ['project_id', 'source', 'source_id'],
        'sort_date_key': 'source_timestamp'
    }))

    for source in config:
        steps.append(('measure.add_outputs_resource', {
            'sheet_id': source.get('sheetid'),
            'gid': source.get('gid'),
            'source_type': source.get('type')
        }))

    steps.append(('measure.remove_resource', {
        'name': 'latest-project-entries'
    }))

    steps.append(('concatenate', {
        'target': {
            'name': 'outputs',
            'path': 'data/outputs.csv'},
        'fields': {
            'source_id': [],
            'source_type': [],
            'source': [],
            'source_timestamp': [],
            'source_email': [],
            'output_title': [],
            'output_type': [],
            'output_organization': [],
            'output_person': [],
            'output_link': [],
            'output_date': []}
    }))

    steps.append(('set_types', {
        'types': {
            'source_id': {
                'type': 'string'
            },
            'source_type': {
                'type': 'string'
            },
            'source': {
                'type': 'string'
            },
            'source_timestamp': {
                'type': 'datetime'
            },
            'source_email': {
                'type': 'string'
            },
            'output_title': {
                'type': 'string'
            },
            'output_organization': {
                'type': 'string'
            },
            'output_person': {
                'type': 'string'
            },
            'output_link': {
                'type': 'string'
            },
            'output_date': {
                'type': 'date'
            }}
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
            'outputs': {
                'resource-name': 'outputs',
                'mode': 'update',
                'update_keys': ['project_id', 'source', 'source_timestamp',
                                'source_id']
            }
        }
    }))

    return steps

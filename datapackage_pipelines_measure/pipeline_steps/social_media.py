import os

from datapackage_pipelines_measure.config import settings


DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'social-media'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:
    for entity in config['twitter']['entities']:
        steps.append(('measure.add_twitter_resource', {
            'entity': entity,
            'project_id': project_id
        }))

    for page in config['facebook']['pages']:
        steps.append(('measure.add_facebook_resource', {
            'entity': page,
            'project_id': project_id
        }))

    steps.append(('concatenate', {
        'target': {
            'name': 'social-media',
            'path': 'data/social-media.csv'},
        'fields': {
            'entity': [],
            'entity_type': [],
            'source': [],
            'date': [],
            'followers': [],
            'mentions': [],
            'interactions': [],
            'impressions': []}
    }))

    steps.append(('set_types', {
        'types': {
            'entity': {
                'type': 'string',
            },
            'entity_type': {
                'type': 'string'
            },
            'source': {
                'type': 'string'
            },
            'date': {
                'type': 'date',
            },
            'followers': {
                'type': 'integer'
            },
            'mentions': {
                'type': 'integer'
            },
            'interactions': {
                'type': 'integer'
            },
            'impressions': {
                'type': 'integer'
            }
        }
    }))

    steps.append(('measure.add_project_name', {'name': project_id}))
    steps.append(('measure.add_timestamp'))
    steps.append(('measure.add_uuid'))

    # Dump to path if in development
    if settings.get('DEVELOPMENT', False):
        steps.append(('dump.to_path', {
            'out-path': '{}/{}'.format(DOWNLOADS_PATH, pipeline_id)
        }))

    steps.append(('dump.to_sql', {
        'engine': settings.DB_ENGINE,
        'tables': {
            'socialmedia': {
                'resource-name': 'social-media',
                'mode': 'update',
                'update_keys': ['entity', 'entity_type',
                                'source', 'project_id', 'date']
            }
        }
    }))

    return steps

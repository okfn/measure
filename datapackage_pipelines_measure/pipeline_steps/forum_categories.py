import os

from datapackage_pipelines_measure.config import settings

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'forum-categories'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:

    steps.append(('measure.datastore_get_latest', {
        'resource-name': 'latest-project-entries',
        'table': 'forum_categories',
        'engine': settings.get('DB_ENGINE'),
        'distinct_on': ['project_id', 'domain', 'source', 'category']
    }))

    for domain_categories in config['discourse-categories']:
        for category in domain_categories['categories']:
            steps.append(('measure.add_discourse_category_resource', {
                'category': category,
                'domain': domain_categories['domain']
            }))

    steps.append(('measure.remove_resource', {
        'name': 'latest-project-entries'
    }))

    steps.append(('concatenate', {
        'target': {
            'name': 'forum-categories',
            'path': 'data/forum-categories.json'},
        'fields': {
            'domain': [],
            'category': [],
            'new_topics': [],
            'new_posts': [],
            'source': [],
            'date': []}
    }))

    steps.append(('set_types', {
        'types': {
            'domain': {
                'type': 'string',
            },
            'category': {
                'type': 'string',
            },
            'source': {
                'type': 'string',
            },
            'new_topics': {
                'type': 'integer'
            },
            'new_posts': {
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
            'forum_categories': {
                'resource-name': 'forum-categories',
                'mode': 'update',
                'update_keys': ['domain', 'category', 'source',
                                'project_id', 'date']
            }
        }
    }))

    return steps

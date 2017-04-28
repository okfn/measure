import os

from datapackage_pipelines.generators import slugify

from datapackage_pipelines_measure.config import settings

DOWNLOADS_PATH = os.path.join(os.path.dirname(__file__), '../../downloads')

label = 'social-media'


def add_steps(steps: list, pipeline_id: str,
              project_id: str, config: dict) -> list:
    for entity in config['twitter']['entities']:
        # for each entity
            # create a resource with
            # - entity: name of entity
            # - entity_type: account, hashtag, page
        steps.append(('measure.add_twitter_resource', {
            'entity': entity
        }))

    steps.append(('concatenate', {
        'target': {
            'name': 'social-media',
            'path': 'data/social-media.json'},
        'fields': {
            'entity': [],
            'entity_type': [],
            'source': []}
    }))

    # steps.append(('set_types', {
    #     'types': {
    #         'repository': {
    #             'type': 'string',
    #         },
    #         'watchers': {
    #             'type': 'integer'
    #         },
    #         'stars': {
    #             'type': 'integer'
    #         }
    #     }
    # }))

    steps.append(('measure.add_project_name', {'name': project_id}))
    steps.append(('measure.add_timestamp'))
    steps.append(('measure.add_uuid'))

    # temporarily dump to path for development
    steps.append(('dump.to_path', {
        'out-path': '{}/{}'.format(DOWNLOADS_PATH, pipeline_id)
    }))

    # steps.append(('dump.to_sql', {
    #     'engine': settings.DB_ENGINE,
    #     'tables': {
    #         'socialmedia': {
    #             'resource-name': 'social-media',
    #             'mode': 'append'
    #         }
    #     }
    # }))

    return steps

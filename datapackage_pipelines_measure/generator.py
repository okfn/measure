import os
import json

from datapackage_pipelines.generators import (
    GeneratorBase,
    steps,
    slugify,
    SCHEDULE_NONE
)

from .config import settings

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SOCIALMEDIA_SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/socialmedia_schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SOCIALMEDIA_SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        metadata_project = slugify(source['project'])

        for k, config in source['config'].items():
            pipeline_id = slugify('{}-{}'.format(source['project'], k))
            schedule = SCHEDULE_NONE

            # Mock pipeline step, to be replaced with real ones later. Likely
            # broken out into separate modules.
            if k == 'social-media':
                pipeline_steps = steps(*[
                    ('add_metadata', {
                        'project': metadata_project,
                        'name': pipeline_id,
                        'github': settings.GITHUB_API_BASE_URL
                    }),
                    ('add_resource', {
                        'name': 'test_resource',
                        'url': 'https://docs.google.com/spreadsheets/d/' +
                        '1vbhTuMDNCmxdo2rPkkya9v6X1f9eyqvSGsY5YcxlcLk/' +
                        'edit#gid=0'
                    }),
                    ('stream_remote_resources', {}),
                    ('measure.capitalise', {}),
                    ('dump.to_path', {
                        'out-path':
                            '{}/downloads/{}'.format(ROOT_PATH, pipeline_id)
                    })
                ])
            elif k == 'code-hosting':
                pipeline_steps = steps(*[
                    ('add_metadata', {
                        'project': metadata_project,
                        'name': pipeline_id
                    }),
                    ('add_resource', {
                        'name': 'test_resource',
                        'url': 'https://docs.google.com/spreadsheets/d/' +
                        '1vbhTuMDNCmxdo2rPkkya9v6X1f9eyqvSGsY5YcxlcLk/' +
                        'edit#gid=0'
                    }),
                    ('stream_remote_resources', {}),
                    ('measure.capitalise', {}),
                    ('dump.to_path', {
                        'out-path':
                            '{}/downloads/{}'.format(ROOT_PATH, pipeline_id)
                    })
                ])
            else:
                log.warn('No {} pipeline generator available for {}'.format(
                    k, metadata_project))
                continue

            pipeline_details = {
                'pipeline': pipeline_steps
            }
            if schedule is not None:
                pipeline_details['schedule']['crontab'] = schedule

            yield pipeline_id, pipeline_details

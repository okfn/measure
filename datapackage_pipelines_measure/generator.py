import os
import json
import pkgutil

from datapackage_pipelines.generators import (
    GeneratorBase,
    steps,
    slugify,
    SCHEDULE_DAILY
)

from . import pipeline_steps

import logging
log = logging.getLogger(__name__)


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/measure_spec_schema.json')


class Generator(GeneratorBase):

    @staticmethod
    def _get_pipeline_steps() -> dict:
        '''
        Discover available pipeline steps under the `pipeline_steps` package.

        Returns a dict of their {label: add_steps} k/v pairs.
        '''
        pkgpath = os.path.dirname(pipeline_steps.__file__)

        pipeline_modules = [getattr(pipeline_steps, name) for _, name, _
                            in pkgutil.iter_modules([pkgpath])]

        available_steps = {}
        for module in pipeline_modules:
            if module.label and module.add_steps:
                available_steps.update({module.label: module.add_steps})

        return available_steps

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        project_id = slugify(source['project'])
        schedule = SCHEDULE_DAILY

        discovered_steps = cls._get_pipeline_steps()

        for k, config in source['config'].items():
            # `k` corresponds with `label` in pipeline_steps module.
            if k in discovered_steps.keys():
                pipeline_id = slugify('{}-{}'.format(project_id, k))

                common_steps = [
                    ('add_metadata', {
                        'project': project_id,
                        'name': pipeline_id
                    })
                ]

                k_steps = discovered_steps[k](common_steps,
                                              pipeline_id,
                                              project_id,
                                              config)
                _steps = steps(*k_steps)
            else:
                log.warn('No {} pipeline generator available for {}'.format(
                    k, project_id))
                continue

            pipeline_details = {
                'pipeline': _steps
            }
            if schedule is not None:
                pipeline_details['schedule'] = {
                    'crontab': schedule
                }

            yield pipeline_id, pipeline_details

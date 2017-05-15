import os
import re
import datetime
import unittest

import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH


class TestMeasureNPMProcessor(unittest.TestCase):

    @requests_mock.mock()
    def test_add_npm_resource_processor_no_latest(self, mock_request):
        '''No latest in db, so fetch info from registry, and api.'''

        day_range = 5
        now = datetime.datetime.now()
        # package created five days ago
        created = now - datetime.timedelta(days=day_range)
        created = created.strftime("%Y-%m-%d")
        mock_registry = {
            'time': {
                'created': created
            }
        }
        mock_api_responses = []
        for day in reversed(range(1, day_range)):
            start = now - datetime.timedelta(days=day)
            start = start.strftime("%Y-%m-%d")
            mock_api_responses.append({
                'json': {
                    'downloads': day,
                    'start': start,
                    'end': start,
                    'package': 'my_package'
                },
                'status_code': 200
            })
        mock_request.get('https://registry.npmjs.org/my_package',
                         json=mock_registry)
        matcher = re.compile('api.npmjs.org/downloads/point/')
        mock_request.get(matcher, mock_api_responses)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'name': 'hello',
            'package': 'my_package',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_npm_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path,
                                (params, datapackage, iter([])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # one resource
        resources = list(spew_res_iter)
        assert len(resources) == 1

        # rows in resource
        rows = list(resources)[0]
        assert len(rows) == 4
        # row asserts
        assert rows[0] == {
            'date': datetime.date.today() - datetime.timedelta(days=4),
            'downloads': 4,
            'package': 'my_package',
            'source': 'npm'
        }
        assert rows[3]['downloads'] == 1
        assert rows[3]['date'] == \
            datetime.date.today() - datetime.timedelta(days=1)

    @requests_mock.mock()
    def test_add_npm_resource_processor_week_old_latest(self, mock_request):
        '''Latest in db for package is a week old, so fetch info from registry,
            and api.'''

        # package created ten days ago
        now = datetime.datetime.now()
        created = now - datetime.timedelta(days=10)
        created = created.strftime("%Y-%m-%d")
        mock_registry = {
            'time': {
                'created': created
            }
        }
        mock_api_responses = []
        for day in reversed(range(1, 7)):
            start = now - datetime.timedelta(days=day)
            start = start.strftime("%Y-%m-%d")
            mock_api_responses.append({
                'json': {
                    'downloads': day,
                    'start': start,
                    'end': start,
                    'package': 'my_package'
                },
                'status_code': 200
            })
        mock_request.get('https://registry.npmjs.org/my_package',
                         json=mock_registry)
        matcher = re.compile('api.npmjs.org/downloads/point/')
        mock_request.get(matcher, mock_api_responses)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'latest-project-entries',
                'schema': {
                    'fields': [
                        {'name': 'date', 'type': 'date'},
                        {'name': 'downloads', 'type': 'int'},
                        {'name': 'package', 'type': 'string'},
                        {'name': 'source', 'type': 'string'},
                    ]
                }
            }]  # nothing here
        }
        params = {
            'name': 'hello',
            'package': 'my_package',
            'project_id': 'my-project'
        }

        def latest_entries_res():
            yield {
                    'date': datetime.date.today() - datetime.timedelta(days=7),
                    'downloads': 7,
                    'package': 'my_package',
                    'source': 'npm'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_npm_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path,
                                (params, datapackage,
                                 iter([latest_entries_res()])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # two resource
        resources = list(spew_res_iter)
        assert len(resources) == 2

        # first resource will look like latest_entries_res
        assert list(resources[0])[0] == next(latest_entries_res())

        # second resource was added by add_npm_resource processor
        rows = resources[1]
        assert len(rows) == 6
        # row asserts
        assert rows[0] == {
            'date': datetime.date.today() - datetime.timedelta(days=6),
            'downloads': 6,
            'package': 'my_package',
            'source': 'npm'
        }
        assert rows[len(rows) - 1]['downloads'] == 1
        assert rows[len(rows) - 1]['date'] == \
            datetime.date.today() - datetime.timedelta(days=1)

    @requests_mock.mock()
    def test_add_npm_resource_processor_latest_is_today(self, mock_request):
        '''Latest in db for package is today, so fetch don't fetch new data.'''

        # package created ten days ago
        now = datetime.datetime.now()
        created = now - datetime.timedelta(days=10)
        created = created.strftime("%Y-%m-%d")
        mock_registry = {
            'time': {
                'created': created
            }
        }
        mock_request.get('https://registry.npmjs.org/my_package',
                         json=mock_registry)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'latest-project-entries',
                'schema': {
                    'fields': [
                        {'name': 'date', 'type': 'date'},
                        {'name': 'downloads', 'type': 'int'},
                        {'name': 'package', 'type': 'string'},
                        {'name': 'source', 'type': 'string'},
                    ]
                }
            }]  # nothing here
        }
        params = {
            'name': 'hello',
            'package': 'my_package',
            'project_id': 'my-project'
        }

        def latest_entries_res():
            yield {
                    'date': datetime.date.today(),
                    'downloads': 1,
                    'package': 'my_package',
                    'source': 'npm'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_npm_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path,
                                (params, datapackage,
                                 iter([latest_entries_res()])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # two resource
        resources = list(spew_res_iter)
        assert len(resources) == 2

        # first resource will look like latest_entries_res
        assert list(resources[0])[0] == next(latest_entries_res())

        # second resource was added by add_npm_resource processor
        rows = resources[1]
        # no rows were added
        assert len(rows) == 0

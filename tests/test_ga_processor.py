import os
import mock
import dateutil
import unittest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


class TestMeasureGAProcessor(unittest.TestCase):

    @mock.patch(
        'datapackage_pipelines_measure.processors.google_utils.discovery')
    def test_add_ga_resource_processor_no_latest(self, mock_discovery):
        '''No latest in db, so populate from GA request.'''

        ga_response = {
            'reports': [
                {
                    'data': {
                        'rows': [
                            {'dimensions': ['20170515'],
                             'metrics': [{'values': ['1', '1', '2.0']}]},
                            {'dimensions': ['20170516'],
                             'metrics': [{'values': ['3', '5', '8.0']}]},
                            {'dimensions': ['20170517'],
                             'metrics': [{'values': ['13', '21', '34.0']}]},
                            {'dimensions': ['20170518'],
                             'metrics': [{'values': ['55', '89', '144.0']}]}
                        ]
                    }
                }
            ]
        }

        mock_discovery \
            .build.return_value \
            .reports.return_value \
            .batchGet.return_value \
            .execute.return_value = ga_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': {
                'url': 'sub.example.com',
                'viewid': '123456'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ga_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # one resource
        resources = list(spew_res_iter)
        assert len(resources) == 1

        # rows in resource
        rows = list(resources)[0]
        assert len(rows) == 4
        # first row asserts
        assert rows[0] == {
            'date': dateutil.parser.parse('2017-05-15').date(),
            'visitors': 1,
            'unique_visitors': 1,
            'avg_time_spent': 2,
            'domain': 'sub.example.com',
            'source': 'ga'
        }
        # last row asserts
        assert rows[len(rows)-1]['visitors'] == 55
        assert rows[len(rows)-1]['unique_visitors'] == 89
        assert rows[len(rows)-1]['avg_time_spent'] == 144
        assert rows[len(rows)-1]['date'] == \
            dateutil.parser.parse('2017-05-18').date()

    @mock.patch(
        'datapackage_pipelines_measure.processors.google_utils.discovery')
    def test_add_ga_resource_processor_latest_week_old(self, mock_discovery):
        '''Latest in db is a week old, so fetch new data.'''

        ga_response = {
            'reports': [
                {
                    'data': {
                        'rows': [
                            {'dimensions': ['20170515'],
                             'metrics': [{'values': ['1', '1', '2.0']}]},
                            {'dimensions': ['20170516'],
                             'metrics': [{'values': ['3', '5', '8.0']}]},
                            {'dimensions': ['20170517'],
                             'metrics': [{'values': ['13', '21', '34.0']}]},
                            {'dimensions': ['20170518'],
                             'metrics': [{'values': ['55', '89', '144.0']}]}
                        ]
                    }
                }
            ]
        }

        mock_discovery \
            .build.return_value \
            .reports.return_value \
            .batchGet.return_value \
            .execute.return_value = ga_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'latest-project-entries',
                'schema': {
                    'fields': [
                        {'name': 'date', 'type': 'date'},
                        {'name': 'visitors', 'type': 'int'},
                        {'name': 'unique_visitors', 'type': 'int'},
                        {'name': 'domain', 'type': 'string'},
                        {'name': 'avg_time_spent', 'type': 'number'},
                        {'name': 'source', 'type': 'string'},
                    ]
                }
            }]
        }
        params = {
            'domain': {
                'url': 'sub.example.com',
                'viewid': '123456'
            }
        }

        def latest_entries_res():
            yield {
                    'date': dateutil.parser.parse('2017-05-14').date(),
                    'visitors': 3,
                    'unique_visitors': 5,
                    'avg_time_spent': 12,
                    'domain': 'sub.example.com',
                    'source': 'ga'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ga_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path,
                                (params, datapackage,
                                 iter([latest_entries_res()])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # two resources
        resources = list(spew_res_iter)
        assert len(resources) == 2

        # first resource will look like latest_entries_res
        assert list(resources[0])[0] == next(latest_entries_res())

        # rows in resource
        rows = list(resources)[1]
        assert len(rows) == 4
        # first row asserts
        assert rows[0] == {
            'date': dateutil.parser.parse('2017-05-15').date(),
            'visitors': 1,
            'unique_visitors': 1,
            'avg_time_spent': 2,
            'domain': 'sub.example.com',
            'source': 'ga'
        }
        # last row asserts
        assert rows[len(rows)-1]['visitors'] == 55
        assert rows[len(rows)-1]['unique_visitors'] == 89
        assert rows[len(rows)-1]['avg_time_spent'] == 144
        assert rows[len(rows)-1]['date'] == \
            dateutil.parser.parse('2017-05-18').date()

    @mock.patch(
        'datapackage_pipelines_measure.processors.google_utils.discovery')
    def test_add_ga_resource_processor_no_row_returned(self, mock_discovery):
        '''No rows returned from GA response.'''

        ga_response = {
            'reports': [
                {
                    'data': {}
                }
            ]
        }

        mock_discovery \
            .build.return_value \
            .reports.return_value \
            .batchGet.return_value \
            .execute.return_value = ga_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'domain': {
                'url': 'sub.example.com',
                'viewid': '123456'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_ga_resource.py')

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
        assert len(rows) == 0

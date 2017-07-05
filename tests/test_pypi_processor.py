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


class TestMeasurePypiProcessor(unittest.TestCase):

    '''These test provide quite good code coverage, but don't test the date
    selection logic that well, or how well the sql statement works. The mock
    will always output predictably, as defined by the test.'''

    @mock.patch(
        'datapackage_pipelines_measure.processors.google_utils.discovery')
    def test_add_pypi_resource_processor_no_latest(self, mock_discovery):
        '''No latest in db, so populate from big query request.'''

        bq_response = {
            'rows': [
                {
                    'f': [
                        {'v': 'my_package'},
                        {'v': '2017-05-14'},
                        {'v': '6'}
                    ]
                },
                {
                    'f': [
                        {'v': 'my_package'},
                        {'v': '2017-05-15'},
                        {'v': '12'}
                    ]
                },
                {
                    'f': [
                        {'v': 'my_package'},
                        {'v': '2017-05-16'},
                        {'v': '24'}
                    ]
                }
            ]
        }

        mock_discovery \
            .build.return_value \
            .jobs.return_value \
            .query.return_value \
            .execute.return_value = bq_response

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
        processor_path = os.path.join(processor_dir, 'add_pypi_resource.py')

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
        assert len(rows) == 3
        # first row asserts
        assert rows[0] == {
            'date': dateutil.parser.parse('2017-05-14').date(),
            'downloads': 6,
            'package': 'my_package',
            'source': 'pypi'
        }
        # last row asserts
        assert rows[len(rows)-1]['downloads'] == 24
        assert rows[len(rows)-1]['date'] == \
            dateutil.parser.parse('2017-05-16').date()

    @mock.patch(
        'datapackage_pipelines_measure.processors.google_utils.discovery')
    def test_add_pypi_resource_processor_latest_week_old(self, mock_discovery):
        '''Latest in db is a week old, so fetch new data.'''

        bq_response = {
            'rows': [
                {
                    'f': [
                        {'v': 'my_package'},
                        {'v': '2017-05-14'},
                        {'v': '6'}
                    ]
                },
                {
                    'f': [
                        {'v': 'my_package'},
                        {'v': '2017-05-15'},
                        {'v': '12'}
                    ]
                },
                {
                    'f': [
                        {'v': 'my_package'},
                        {'v': '2017-05-16'},
                        {'v': '24'}
                    ]
                }
            ]
        }

        mock_discovery \
            .build.return_value \
            .jobs.return_value \
            .query.return_value \
            .execute.return_value = bq_response

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
            }]
        }
        params = {
            'name': 'hello',
            'package': 'my_package',
            'project_id': 'my-project'
        }

        def latest_entries_res():
            yield {
                    'date': dateutil.parser.parse('2017-05-13').date(),
                    'downloads': 3,
                    'package': 'my_package',
                    'source': 'pypi'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_pypi_resource.py')

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
        assert len(rows) == 3
        # first row asserts
        assert rows[0] == {
            'date': dateutil.parser.parse('2017-05-14').date(),
            'downloads': 6,
            'package': 'my_package',
            'source': 'pypi'
        }
        # last row asserts
        assert rows[len(rows)-1]['downloads'] == 24
        assert rows[len(rows)-1]['date'] == \
            dateutil.parser.parse('2017-05-16').date()

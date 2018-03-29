import os
import mock
import dateutil
import pytest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


class TestMeasureGAProcessor(object):

    @pytest.mark.usefixtures('full_ga_response')
    def test_add_ga_resource_processor_no_latest(self):
        '''No latest in db, so populate from GA request.'''

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
            'page_path': '/',
            'visitors': 1,
            'unique_visitors': 1,
            'avg_time_spent': 2,
            'domain': 'sub.example.com',
            'source': 'ga',
            'pageviews': 100,
        }
        # last row asserts
        assert rows[len(rows)-1]['visitors'] == 55
        assert rows[len(rows)-1]['unique_visitors'] == 89
        assert rows[len(rows)-1]['avg_time_spent'] == 144
        assert rows[len(rows)-1]['date'] == \
            dateutil.parser.parse('2017-05-18').date()

    @pytest.mark.usefixtures('full_ga_response')
    def test_add_ga_resource_processor_latest_week_old(self):
        '''Latest in db is a week old, so fetch new data.'''

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
                        {'name': 'page_path', 'type': 'string'},
                        {'name': 'avg_time_spent', 'type': 'number'},
                        {'name': 'source', 'type': 'string'},
                        {'name': 'pageviews', 'type': 'int'},
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
                    'page_path': '/',
                    'visitors': 3,
                    'unique_visitors': 5,
                    'avg_time_spent': 12,
                    'domain': 'sub.example.com',
                    'source': 'ga',
                    'pageviews': 100,
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
            'page_path': '/',
            'visitors': 1,
            'unique_visitors': 1,
            'avg_time_spent': 2,
            'domain': 'sub.example.com',
            'source': 'ga',
            'pageviews': 100,
        }
        # last row asserts
        assert rows[len(rows)-1]['visitors'] == 55
        assert rows[len(rows)-1]['unique_visitors'] == 89
        assert rows[len(rows)-1]['avg_time_spent'] == 144
        assert rows[len(rows)-1]['date'] == \
            dateutil.parser.parse('2017-05-18').date()

    @pytest.mark.usefixtures('empty_ga_response')
    def test_add_ga_resource_processor_no_row_returned(self):
        '''No rows returned from GA response.'''

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


@pytest.fixture
def full_ga_response():
    ga_response = {
        'reports': [
            {
                'data': {
                    'rows': [
                        {'dimensions': ['20170515', 'sub.example.com', '/'],
                         'metrics': [{'values': ['1', '1', '2.0', '100']}]},
                        {'dimensions': ['20170516', 'sub.example.com', '/'],
                         'metrics': [{'values': ['3', '5', '8.0', '120']}]},
                        {'dimensions': ['20170517', 'sub.example.com', '/'],
                         'metrics': [{'values': ['13', '21', '34.0', '30']}]},
                        {'dimensions': ['20170518', 'sub.example.com', '/'],
                         'metrics': [{'values': ['55', '89', '144.0', '50']}]}
                    ]
                }
            }
        ]
    }

    yield from _mock_google_utils(ga_response)


@pytest.fixture
def empty_ga_response():
    ga_response = {
        'reports': [
            {
                'data': {}
            }
        ]
    }

    yield from _mock_google_utils(ga_response)


def _mock_google_utils(ga_response):
    with mock.patch('datapackage_pipelines_measure.processors.google_utils.discovery') as mock_discovery:
        mock_discovery \
            .build.return_value \
            .reports.return_value \
            .batchGet.return_value \
            .execute.return_value = ga_response
        yield mock_discovery

import os
import dateutil
import datetime
import unittest

import simplejson
import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


class TestMeasurePackagistProcessor(unittest.TestCase):

    @requests_mock.mock()
    def test_add_packagist_resource_processor(self, mock_request):
        '''No latest in database. Use all returned data.'''
        # mock the packagist response
        mock_packagist_response = {"labels":["2017-04-17","2017-04-18","2017-04-19","2017-04-20","2017-04-21"],"values":[1,2,3,4,5],"average":"daily"}  # noqa
        mock_request.get('https://packagist.org/packages/organization/mypackage/stats/all.json',  # noqa
                         json=mock_packagist_response)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'package': 'organization/mypackage'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_packagist_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, []))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'organization-mypackage'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['package', 'source', 'date', 'downloads']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        rows = list(spew_res_iter_contents[0])
        assert len(rows) == 5
        assert rows[0] == \
            {
                'package': 'mypackage',
                'source': 'packagist',
                'downloads': 1,
                'date': dateutil.parser.parse('2017-04-17').date()
            }
        assert rows[4] == \
            {
                'package': 'mypackage',
                'source': 'packagist',
                'downloads': 5,
                'date': dateutil.parser.parse('2017-04-21').date()
            }

    @requests_mock.mock()
    def test_add_packagist_resource_processor_latest_exists(self,
                                                            mock_request):
        '''Latest in database exists in returned data. Use only more recent.'''
        # mock the packagist response
        mock_packagist_response = {"labels":["2017-04-17","2017-04-18","2017-04-19","2017-04-20","2017-04-21"],"values":[1,2,3,4,5],"average":"daily"}  # noqa
        mock_request.get('https://packagist.org/packages/organization/mypackage/stats/all.json',  # noqa
                         json=mock_packagist_response)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'latest-project-entries',
                'schema': {
                    'fields': [
                        {'name': 'source', 'type': 'string'},
                        {'name': 'date', 'type': 'date'},
                        {'name': 'package', 'type': 'string'},
                        {'name': 'downloads', 'type': 'int'}
                    ]
                }
            }]
        }
        params = {
            'package': 'organization/mypackage'
        }

        # latest is in returned data
        def latest_entries_res():
            yield {
                    'date': dateutil.parser.parse('2017-04-19').date(),
                    'downloads': 2,
                    'package': 'mypackage',
                    'source': 'packagist'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_packagist_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path, (params, datapackage,
                                                 iter([latest_entries_res()])))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 2
        assert dp_resources[1]['name'] == 'organization-mypackage'
        field_names = \
            [field['name'] for field in dp_resources[1]['schema']['fields']]
        assert field_names == ['package', 'source', 'date', 'downloads']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 2
        rows = list(spew_res_iter_contents[1])
        assert len(rows) == 3  # updated only 3 recent rows
        assert rows[0] == \
            {
                'package': 'mypackage',
                'source': 'packagist',
                'downloads': 3,  # value updated from api
                'date': dateutil.parser.parse('2017-04-19').date()
            }
        assert rows[2] == \
            {
                'package': 'mypackage',
                'source': 'packagist',
                'downloads': 5,
                'date': dateutil.parser.parse('2017-04-21').date()
            }

    @requests_mock.Mocker()
    def test_add_packagist_resource_not_json(self, mock_request):
        # Mock API responses
        mock_request.get('https://packagist.org/packages/organization/mypackage/stats/all.json',  # noqa
                         text="This is not json.")

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'package': 'organization/mypackage'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_packagist_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(simplejson.scanner.JSONDecodeError):
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)

    @requests_mock.Mocker()
    def test_add_packagist_resource_bad_status(self, mock_request):
        # Mock API responses
        error_msg = 'Hi, there was a problem with your request.'
        mock_request.get('https://packagist.org/packages/organization/mypackage/stats/all.json',  # noqa
                         text=error_msg, status_code=401)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'package': 'organization/mypackage'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_packagist_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(Exception) as cm:
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)
        self.assertEqual(str(cm.exception), error_msg)

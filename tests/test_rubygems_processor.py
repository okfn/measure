import os
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


class TestMeasureRubygemsProcessor(unittest.TestCase):

    @requests_mock.mock()
    def test_add_rubygems_resource_processor(self, mock_request):
        '''No latest in database. Get today's data.'''
        # mock the rubygems response
        mock_rubygems_response = {
            'name': 'mygem',
            'downloads': 271,
            'version': '0.3.1',
            'version_downloads': 170
        }
        mock_request.get('https://rubygems.org/api/v1/gems/mygem.json',
                         json=mock_rubygems_response)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'gem_id': 'mygem'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_rubygems_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, []))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'mygem'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['source', 'date', 'package', 'downloads',
                               'total_downloads']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [{
                'package': 'mygem',
                'source': 'rubygems',
                'total_downloads': 271,
                'date': datetime.date.today()
            }]

    @requests_mock.mock()
    def test_add_rubygems_resource_processor_latest_yesterday(self,
                                                              mock_request):
        '''Latest was yesterday. Get today's data, and add `downloads`.'''
        # mock the rubygems response
        mock_rubygems_response = {
            'name': 'mygem',
            'downloads': 271,
            'version': '0.3.1',
            'version_downloads': 170
        }
        mock_request.get('https://rubygems.org/api/v1/gems/mygem.json',
                         json=mock_rubygems_response)

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
                        {'name': 'downloads', 'type': 'int'},
                        {'name': 'total_downloads', 'type': 'int'}
                    ]
                }
            }]
        }
        params = {
            'gem_id': 'mygem'
        }

        # latest is yest
        def latest_entries_res():
            yield {
                    'date': datetime.date.today() - datetime.timedelta(days=1),
                    'downloads': 7,
                    'total_downloads': 265,
                    'package': 'mygem',
                    'source': 'rubygems'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_rubygems_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage,
                                            iter([latest_entries_res()])))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 2
        assert dp_resources[0]['name'] == 'latest-project-entries'
        assert dp_resources[1]['name'] == 'mygem'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['source', 'date', 'package', 'downloads',
                               'total_downloads']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 2
        assert list(spew_res_iter_contents[1]) == \
            [{
                'package': 'mygem',
                'source': 'rubygems',
                'downloads': 6,
                'total_downloads': 271,
                'date': datetime.date.today()
            }]

    @requests_mock.mock()
    def test_add_rubygems_resource_processor_latest_today(self,
                                                              mock_request):
        '''Latest is today. Get today's data, ensure `downloads` is
        retained.'''
        # mock the rubygems response
        mock_rubygems_response = {
            'name': 'mygem',
            'downloads': 271,
            'version': '0.3.1',
            'version_downloads': 170
        }
        mock_request.get('https://rubygems.org/api/v1/gems/mygem.json',
                         json=mock_rubygems_response)

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
                        {'name': 'downloads', 'type': 'int'},
                        {'name': 'total_downloads', 'type': 'int'}
                    ]
                }
            }]
        }
        params = {
            'gem_id': 'mygem'
        }

        # latest is yest
        def latest_entries_res():
            yield {
                    'date': datetime.date.today(),
                    'downloads': 7,
                    'total_downloads': 265,
                    'package': 'mygem',
                    'source': 'rubygems'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_rubygems_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage,
                                            iter([latest_entries_res()])))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert dp_resources[1]['name'] == 'mygem'

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert list(spew_res_iter_contents[1]) == \
            [{
                'package': 'mygem',
                'source': 'rubygems',
                'downloads': 7,
                'total_downloads': 271,
                'date': datetime.date.today()
            }]

    @requests_mock.Mocker()
    def test_add_rubygems_resource_not_json(self, mock_request):
        # Mock API responses
        mock_request.get('https://rubygems.org/api/v1/gems/mygem404.json',
                         text="This is not json.")

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'gem_id': 'mygem404'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_rubygems_resource.py')

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
    def test_add_rubygems_resource_bad_status(self, mock_request):
        # Mock API responses
        mock_request.get('https://rubygems.org/api/v1/gems/mygem401.json',
                         text='Hi, there was a problem with your request.',
                         status_code=401)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'gem_id': 'mygem401'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_rubygems_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(Exception):
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)

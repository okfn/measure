# -*- coding: utf-8 -*-

import os
import re
import json
import datetime
import unittest

import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    ProcessorFixtureTestsBase,
    rejsonize,
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH


class TestMeasureProcessors(unittest.TestCase):

    @requests_mock.mock()
    def test_my_processor(self, mock_request):
        # mock the github repsonse
        mock_github_response = {
            'name': 'my-repository',
            'subscribers_count': 4,
            'stargazers_count': 1
        }
        mock_request.get(requests_mock.ANY, json=mock_github_response)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'name': 'hello',
            'repo': 'my_github_repo',
            'map_fields': {
                'repository': 'name',
                'watchers': 'subscribers_count',
                'stars': 'stargazers_count'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_github_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'hello'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['repository', 'watchers', 'stars']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [{'repository': 'my-repository', 'watchers': 4, 'stars': 1}]


class MeasureProcessorsFixturesTest(ProcessorFixtureTestsBase):

    def _get_procesor_env(self):
        return ENV

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH,
                            'datapackage_pipelines_measure',
                            'processors',
                            processor.strip() + '.py')

    @staticmethod
    def _get_first_line(data):
        '''Return the first line of `data` as a python object.'''
        if len(data) > 0:
            data = data[0]
            data = data.split('\n')
            actual = data[0]
            rj_actual = rejsonize(actual)
            return json.loads(rj_actual)


for filename, testfunc in MeasureProcessorsFixturesTest(
                            os.path.join(os.path.dirname(__file__), 'fixtures')
                            ).get_tests():
    globals()['test_processors_%s' % filename] = testfunc


class MeasureProcessorsFixturesTest_UUID(MeasureProcessorsFixturesTest):

    def test_fixture(self, output, dp_out, *args):
        """Test `id` is in output data."""
        (actual_dp, *actual_data) = output.split('\n\n', 1)
        actual_json = self._get_first_line(actual_data)

        assert actual_dp == dp_out, \
            "unexpected value for output datapackage: {}".format(actual_dp)

        assert 'id' in actual_json
        uuid_regexp = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") # noqa
        assert uuid_regexp.match(actual_json['id']), \
            "id must match uuid regexp"


for filename, testfunc in MeasureProcessorsFixturesTest_UUID(
                    os.path.join(os.path.dirname(__file__), 'fixtures_uuid')
                    ).get_tests():
    globals()['test_processors_%s' % filename] = testfunc


class MeasureProcessorsFixturesTest_Timestamp(MeasureProcessorsFixturesTest):

    def test_fixture(self, output, dp_out, *args):
        """Test `timestamp` is in the output data."""
        (actual_dp, *actual_data) = output.split('\n\n', 1)
        actual_json = self._get_first_line(actual_data)

        assert actual_dp == dp_out, \
            "unexpected value for output datapackage: {}".format(actual_dp)

        assert 'timestamp' in actual_json
        try:
            datetime.datetime.strptime(actual_json['timestamp'],
                                       '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            assert False, \
                "Timestamp must be a datetime in the correct format"


for filename, testfunc in MeasureProcessorsFixturesTest_Timestamp(
                    os.path.join(os.path.dirname(__file__),
                                 'fixtures_timestamp')).get_tests():
    globals()['test_processors_%s' % filename] = testfunc

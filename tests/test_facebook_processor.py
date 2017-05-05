import os
import mock
import unittest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


class MockFacebookGraph():

    def __init__(self):
        self.access_token = ''

    def request(self, path, args):
        since = args['since']
        return {
            "data": [
                {
                    "name": "page_fans",
                    "period": "lifetime",
                    "values": [
                        {
                          "value": 16689,
                          "end_time": since
                        }
                    ]
                }
            ]
        }


my_mock_api = MockFacebookGraph()


class TestMeasureFacebookProcessor(unittest.TestCase):

    @mock.patch('facebook.GraphAPI')
    def test_add_facebook_resource_processor_page(self, mock_api):
        '''Test facebook processor handles page entities (MyPage). No stored
        result.'''

        mock_api.return_value = my_mock_api

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'MyPage'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_facebook_resource.py')

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
        assert dp_resources[0]['name'] == 'mypage'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['entity', 'entity_type',
                               'source', 'followers']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [{
                'entity': 'MyPage',
                'entity_type': 'page',
                'source': 'facebook',
                'followers': 16689
            }]

    def test_add_facebook_resource_processor_nopagetoken(self):
        '''Test facebook processor handles when no page token is available.'''

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'NoPage'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_facebook_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        error_msg = 'No Facebook Page Access Token found for page:'
        ' "NoPage" in settings'
        with self.assertRaises(RuntimeError, msg=error_msg):
            mock_processor_test(processor_path, (params, datapackage, []))

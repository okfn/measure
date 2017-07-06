import os
import mock
import datetime
import unittest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


class MockFacebookGraph():

    def __init__(self, respond_with=None):
        self.respond_with = respond_with
        self.access_token = ''

    def request(self, path, args):
        if self.respond_with is None:
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
                    },
                    {
                        "name": "page_impressions",
                        "period": "day",
                        "values": [
                            {
                                "value": 1,
                                "end_time": "2014-09-02T07:00:00+0000"
                            },
                            {
                                "value": 1,
                                "end_time": since
                            }
                        ]
                    },
                    {
                        "name": "page_stories",
                        "period": "day",
                        "values": [
                            {
                                "value": 2,
                                "end_time": "2014-09-02T07:00:00+0000"
                            },
                            {
                                "value": 3,
                                "end_time": since
                            }
                        ]
                    },
                    {
                        "name": "page_stories_by_story_type",
                        "period": "day",
                        "values": [
                            {
                                "value": {
                                    "user post": 0,
                                    "page post": 0,
                                    "checkin": 0,
                                    "fan": 0,
                                    "question": 0,
                                    "coupon": 0,
                                    "event": 0,
                                    "mention": 5,
                                    "other": 0
                                },
                                "end_time": "2014-09-02T07:00:00+0000"
                            },
                            {
                                "value": {
                                    "user post": 0,
                                    "page post": 0,
                                    "checkin": 0,
                                    "fan": 0,
                                    "question": 0,
                                    "coupon": 0,
                                    "event": 0,
                                    "mention": 8,
                                    "other": 0
                                },
                                "end_time": since
                            }
                        ]
                    }
                ]
            }
        elif self.respond_with == 'NODATA':
            # No `data` attribute
            return {}


my_mock_api = MockFacebookGraph()
my_mock_api_no_response = MockFacebookGraph(respond_with='NODATA')


class TestMeasureFacebookProcessor(unittest.TestCase):

    @mock.patch('facebook.GraphAPI')
    def test_add_facebook_resource_processor_page(self, mock_api):
        '''Test facebook processor handles page properties.
        '''

        mock_api.return_value = my_mock_api

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'MyPage',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_facebook_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path, (params, datapackage, []))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        # One resource
        assert len(spew_res_iter_contents) == 1
        # With one row
        assert len(spew_res_iter_contents[0]) == 1
        row = list(spew_res_iter_contents[0])[0]
        log.debug(row)
        assert row['followers'] == 16689
        assert row['mentions'] == 13
        assert row['interactions'] == 5
        assert row['impressions'] == 2
        assert row['date'] == \
            datetime.date.today() - datetime.timedelta(days=1)

    @mock.patch('facebook.GraphAPI')
    def test_add_facebook_resource_processor_page_no_data(self, mock_api):
        '''Test facebook processor raises exception is no data is returned from
        api.'''

        mock_api.return_value = my_mock_api_no_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'MyPage',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_facebook_resource.py')

        # Trigger the processor with our mock `ingest` will return an exception
        error_msg = 'Facebook request returned no data.'
        with self.assertRaises(ValueError) as cm:
            mock_processor_test(processor_path, (params, datapackage, []))

        self.assertEqual(str(cm.exception), error_msg)

    def test_add_facebook_resource_processor_nopagetoken(self):
        '''Test facebook processor handles when no page token is available.'''

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'NoPage',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_facebook_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        error_msg = '\'No Facebook Page Access Token found for page ' \
            '"NoPage" in settings\''
        with self.assertRaises(KeyError) as cm:
            mock_processor_test(processor_path, (params, datapackage, []))

        self.assertEqual(str(cm.exception), error_msg)

import os
import mock
import unittest
import datetime

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors
from datapackage_pipelines_measure.config import settings

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


my_mock_api = MockFacebookGraph()


class MockDatastore():
    '''A Mock Datastore.'''

    def __init__(self, latest):
        self.latest = latest

    def get_latest_from_table(self, *args):
        '''Return the object saved during init.'''
        return self.latest


class TestMeasureFacebookProcessor(unittest.TestCase):

    @mock.patch('datapackage_pipelines_measure.datastore.get_datastore')
    @mock.patch('facebook.GraphAPI')
    def test_add_facebook_resource_processor_page_today(self, mock_api,
                                                        mock_datastore):
        '''Test facebook processor handles page properties when latest stored
        result was run today.

        Use the stored result.
        '''

        mock_api.return_value = my_mock_api
        stored_latest = {
            'followers': 15000,
            'mentions': 5,
            'interactions': 20,
            'impressions': 250,
            'timestamp': datetime.datetime.now()
        }
        mock_datastore.return_value = MockDatastore(stored_latest)

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
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        resource = list(spew_res_iter_contents[0])[0]
        # followers is updated from api
        assert resource['followers'] == 16689
        # the others are updated from today's stored value
        assert resource['mentions'] == 5
        assert resource['interactions'] == 20
        assert resource['impressions'] == 250

    @mock.patch('datapackage_pipelines_measure.datastore.get_datastore')
    @mock.patch('facebook.GraphAPI')
    def test_add_facebook_resource_processor_properties_before_today(
        self,
        mock_api,
        mock_datastore
    ):
        '''Test facebook processor correctly accumulates properties when latest
        stored result was run before today.

        Add today's result to the latest stored result.
        '''

        # mock the facebook api response
        mock_api.return_value = my_mock_api
        stored_latest = {
            'mentions': 5,
            'interactions': 20,
            'impressions': 120,
            'followers': 15000,
            'timestamp': datetime.datetime.now() - datetime.timedelta(days=3)
        }
        mock_datastore.return_value = MockDatastore(stored_latest)

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
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        resource = list(spew_res_iter_contents[0])[0]
        assert resource['followers'] == 16689
        assert resource['mentions'] == 18
        assert resource['interactions'] == 25
        assert resource['impressions'] == 122

    @mock.patch('facebook.GraphAPI')
    def test_add_facebook_resource_processor_page(self, mock_api):
        '''Test facebook processor handles page entities (MyPage). No stored
        result.'''

        mock_api.return_value = my_mock_api
        # Set default start date to something sane to prevent multiple
        # responses accumulating too many values. As end_date will be
        # yesterday, we want the day before yesterday as the start date.
        before_yesterday = datetime.date.today() - datetime.timedelta(days=2)
        mock_start_date = before_yesterday.strftime('%Y-%m-%d')
        settings.FACEBOOK_API_DEFAULT_START_DATE = mock_start_date

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
        assert field_names == ['entity', 'entity_type', 'source', 'followers',
                               'mentions', 'interactions', 'impressions']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [{
                'entity': 'MyPage',
                'entity_type': 'page',
                'source': 'facebook',
                'followers': 16689,
                'mentions': 13,
                'interactions': 5,
                'impressions': 2
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
        error_msg = 'No Facebook Page Access Token found for page:'
        ' "NoPage" in settings'
        with self.assertRaises(RuntimeError, msg=error_msg):
            mock_processor_test(processor_path, (params, datapackage, []))

import os
import datetime
import unittest
import mock
from collections import namedtuple

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


Status = namedtuple('Status',
                    [
                        'screen_name',
                        'favorite_count',
                        'retweet_count',
                        'created_at'
                    ])


class MockTwitterAPI():

    def get_user(self, account_name):
        User = namedtuple('User', 'followers_count')
        return User(5)

    def search(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        return [
            Status('okfnlabs', 2, 5, yesterday),
            Status('anonymous', 3, 6, yesterday)
        ]

    def user_timeline(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        before_yesterday = yesterday - datetime.timedelta(days=1)
        return [
            Status('okfnlabs', 2, 5, yesterday),
            Status('okfnlabs', 1, 7, yesterday),
            Status('anonymous', 3, 6, before_yesterday),
            Status('my_user', 1, 1, datetime.datetime.now())
        ]


my_mock_api = MockTwitterAPI()


def get_cursor_items_iter(items):
    class MockCursorIterable():

        def __init__(self, items):
            self._items = items
            self.counter = 0

        def next(self):
            if self.counter >= len(self._items):
                raise StopIteration
            item = self._items[self.counter]
            self.counter += 1
            return item

    return MockCursorIterable(items)


class TestMeasureTwitterProcessor(unittest.TestCase):

    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_account(
        self,
        mock_api,
        mock_auth,
        mock_cursor,
    ):
        '''Test twitter processor handles user account (@myuser) properties.'''

        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = my_mock_api
        mock_cursor.return_value.items.side_effect = [
            get_cursor_items_iter(my_mock_api.search()),
            get_cursor_items_iter(my_mock_api.user_timeline())
        ]

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': '@myuser',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_twitter_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = \
            mock_processor_test(processor_path, (params, datapackage, []))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)

        # One rows in first resource
        assert len(spew_res_iter_contents[0]) == 1

        # Get row from resource
        first_row = list(spew_res_iter_contents[0])[0]
        # followers is updated from api
        assert first_row['date'] == \
            datetime.date.today() - datetime.timedelta(days=1)
        # the others are updated from today's stored result
        assert first_row['mentions'] == 2
        assert first_row['interactions'] == 15
        assert first_row['followers'] == 5

    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_hashtag(self, mock_api,
                                                    mock_auth, mock_cursor):
        '''Test twitter processor handles hashtag entities (#myhashtag).'''
        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = my_mock_api
        yesterday = \
            datetime.datetime.now() - datetime.timedelta(days=1)
        yesterdays_statuses = [
            Status('okfnlabs', 1, 5, yesterday),
            Status('anonymous', 3, 0, yesterday),
            Status('anonymous', 3, 8, yesterday)
        ]
        mock_cursor.return_value.items.side_effect = [
            get_cursor_items_iter(yesterdays_statuses)
        ]

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': '#myhashtag',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_twitter_resource.py')

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
        assert dp_resources[0]['name'] == 'hash-myhashtag'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['entity', 'entity_type',
                               'source', 'date', 'mentions',
                               'interactions', 'followers']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents[0]) == 1

        first_row = spew_res_iter_contents[0][0]
        assert first_row == \
            {
                'entity': '#myhashtag',
                'entity_type': 'hashtag',
                'source': 'twitter',
                'followers': None,
                'mentions': 3,
                'interactions': 20,
                'date': datetime.date.today() - datetime.timedelta(days=1)
            }

    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_url(self, mock_api,
                                                mock_auth, mock_cursor):
        '''Test twitter processor handles url entities (url:<term>).'''
        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = my_mock_api
        yesterday = \
            datetime.datetime.now() - datetime.timedelta(days=1)
        yesterdays_statuses = [
            Status('okfnlabs', 1, 5, yesterday),
            Status('anonymous', 3, 0, yesterday),
            Status('anonymous', 3, 8, yesterday)
        ]
        mock_cursor.return_value.items.side_effect = [
            get_cursor_items_iter(yesterdays_statuses)
        ]

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'url:example.com',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_twitter_resource.py')

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
        assert dp_resources[0]['name'] == 'url-example-com'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['entity', 'entity_type',
                               'source', 'date', 'mentions',
                               'interactions', 'followers']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents[0]) == 1

        first_row = spew_res_iter_contents[0][0]
        assert first_row == \
            {
                'entity': 'url:example.com',
                'entity_type': 'url',
                'source': 'twitter',
                'followers': None,
                'mentions': 3,
                'interactions': 20,
                'date': datetime.date.today() - datetime.timedelta(days=1)
            }

    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_nonentity(self, mock_api,
                                                      mock_auth):
        '''Test twitter processor handles non-entities properly (neither a user
        nor hashtag).'''

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'entity': 'non-entity',
            'project_id': 'my-project'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_twitter_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        error_msg = 'Entity, "non-entity", must be an @account or a #hashtag'
        with self.assertRaises(ValueError, msg=error_msg):
            mock_processor_test(processor_path, (params, datapackage, []))

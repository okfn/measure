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

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH


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


class MockDatastore():
    '''A Mock Datastore.'''

    def __init__(self, latest):
        self.latest = latest

    def get_latest_from_table(self, *args):
        '''Return the object saved during init.'''
        return self.latest


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
            get_cursor_items_iter(my_mock_api.user_timeline()),
            get_cursor_items_iter(my_mock_api.search()),
            get_cursor_items_iter(my_mock_api.user_timeline()),
            get_cursor_items_iter(my_mock_api.search()),
            get_cursor_items_iter(my_mock_api.user_timeline()),
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

        # Three rows in first resource
        assert len(spew_res_iter_contents[0]) == 3

        # Get first row from resource (today)
        first_row = list(spew_res_iter_contents[0])[0]
        # followers is updated from api
        assert first_row['date'] == datetime.date.today()
        assert first_row['followers'] == 5
        # the others are updated from today's stored result
        assert first_row['mentions'] == 2
        assert first_row['interactions'] == 0

        # Get second row from resource (yesterday)
        second_row = list(spew_res_iter_contents[0])[1]
        # followers is updated from api
        assert second_row['date'] == \
            datetime.date.today() - datetime.timedelta(days=1)
        # the others are updated from today's stored result
        assert second_row['mentions'] == 2
        assert second_row['interactions'] == 15

        # Get third row from resource (day before yesterday)
        third_row = list(spew_res_iter_contents[0])[2]
        # followers is updated from api
        assert third_row['date'] == \
            datetime.date.today() - datetime.timedelta(days=2)
        # the others are updated from today's stored result
        assert third_row['mentions'] == 2
        assert third_row['interactions'] == 9

    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_hashtag(self, mock_api,
                                                    mock_auth, mock_cursor):
        '''Test twitter processor handles hashtag entities (#myhashtag).'''
        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = my_mock_api
        today = datetime.date.today()
        todays_statuses = [
            Status('okfnlabs', 2, 5, today),
            Status('anonymous', 3, 6, today)
        ]
        yesterday = \
            datetime.datetime.now() - datetime.timedelta(days=1)
        yesterdays_statuses = [
            Status('okfnlabs', 1, 5, yesterday),
            Status('anonymous', 3, 0, yesterday),
            Status('anonymous', 3, 8, yesterday)
        ]
        day_before_yesterday = yesterday - datetime.timedelta(days=1)
        before_yesterdays_statuses = [
            Status('okfnlabs', 2, 5, day_before_yesterday)
        ]
        mock_cursor.return_value.items.side_effect = [
            get_cursor_items_iter(todays_statuses),
            get_cursor_items_iter(yesterdays_statuses),
            get_cursor_items_iter(before_yesterdays_statuses)
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
        assert len(spew_res_iter_contents[0]) == 3

        first_row = spew_res_iter_contents[0][0]
        assert first_row == \
            {
                'entity': '#myhashtag',
                'entity_type': 'hashtag',
                'followers': None,
                'source': 'twitter',
                'mentions': 2,
                'interactions': 16,
                'date': datetime.date.today()
            }
        second_row = spew_res_iter_contents[0][1]
        assert second_row == \
            {
                'entity': '#myhashtag',
                'entity_type': 'hashtag',
                'source': 'twitter',
                'mentions': 3,
                'interactions': 20,
                'date': datetime.date.today() - datetime.timedelta(days=1)
            }
        third_row = spew_res_iter_contents[0][2]
        assert third_row == \
            {
                'entity': '#myhashtag',
                'entity_type': 'hashtag',
                'source': 'twitter',
                'mentions': 1,
                'interactions': 7,
                'date': datetime.date.today() - datetime.timedelta(days=2)
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

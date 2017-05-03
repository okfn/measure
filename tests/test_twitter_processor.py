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


class MockTwitterAPI():

    def get_user(self, account_name):
        User = namedtuple('User', 'followers_count')
        return User(5)

    def search(self):
        Status = namedtuple('Status', 'screen_name')
        return iter([
            Status('okfnlabs'),
            Status('anonymous')
        ])


class MockTwitterCursor():

    def __init__(self):
        Status = namedtuple('Status', 'screen_name')
        self._items = [
            Status('okfnlabs'),
            Status('anonymous')
        ]

    def items(self):
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

        return MockCursorIterable(self._items)


class MockDatastore():
    '''A Mock Datastore.'''

    def __init__(self, latest):
        self.latest = latest

    def get_latest_from_table(self, *args):
        '''Return the object saved during init.'''
        return self.latest


class TestMeasureTwitterProcessor(unittest.TestCase):

    @mock.patch('datapackage_pipelines_measure.datastore.get_datastore')
    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_mentions_today(
        self,
        mock_api,
        mock_auth,
        mock_cursor,
        mock_datastore
    ):
        '''Test twitter processor handles user account (@myuser) mentions when
        latest stored result was run today.

        Use the stored result.
        '''

        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = MockTwitterAPI()
        mock_cursor.return_value = MockTwitterCursor()
        stored_latest = {
            'mentions': 5,
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
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert list(spew_res_iter_contents[0])[0]['mentions'] == 5

    @mock.patch('datapackage_pipelines_measure.datastore.get_datastore')
    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_mentions_before_today(
        self,
        mock_api,
        mock_auth,
        mock_cursor,
        mock_datastore
    ):
        '''Test twitter processor handles user account entities (@myuser)
        mentions when latest stored result was run before today.

        Add today's result to the latest stored result.
        '''

        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = MockTwitterAPI()
        mock_cursor.return_value = MockTwitterCursor()
        stored_latest = {
            'mentions': 5,
            'timestamp': datetime.datetime.now() - datetime.timedelta(days=2)
        }
        mock_datastore.return_value = MockDatastore(stored_latest)

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
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert list(spew_res_iter_contents[0])[0]['mentions'] == 7

    @mock.patch('datapackage_pipelines_measure.datastore.get_datastore')
    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_user(self, mock_api,
                                                 mock_auth, mock_cursor,
                                                 mock_datastore):
        '''Test twitter processor handles user account entities (@myuser).
        No stored result.'''

        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = MockTwitterAPI()
        mock_cursor.return_value = MockTwitterCursor()
        mock_datastore.return_value = MockDatastore(None)

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
            mock_processor_test(processor_path,
                                (params, datapackage, []))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'at-myuser'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['entity', 'entity_type',
                               'source', 'followers', 'mentions']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [{
                'entity': '@myuser',
                'entity_type': 'account',
                'followers': 5,
                'source': 'twitter',
                'mentions': 2
            }]

    @mock.patch('datapackage_pipelines_measure.datastore.get_datastore')
    @mock.patch('tweepy.Cursor')
    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_hashtag(self, mock_api,
                                                    mock_auth, mock_cursor,
                                                    mock_datastore):
        '''Test twitter processor handles hashtag entities (#myhashtag).'''
        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = MockTwitterAPI()
        mock_cursor.return_value = MockTwitterCursor()
        mock_datastore.return_value = MockDatastore(None)

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
                               'source', 'followers', 'mentions']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [{
                'entity': '#myhashtag',
                'entity_type': 'hashtag',
                'followers': None,
                'source': 'twitter',
                'mentions': 2
            }]

    @mock.patch('tweepy.auth.AppAuthHandler')
    @mock.patch('tweepy.API')
    def test_add_twitter_resource_processor_nonentity(self, mock_api,
                                                      mock_auth):
        '''Test twitter processor handles non-entities properly (neither a user
        nor hashtag).'''
        # mock the twitter api response
        mock_auth.return_value = 'authed'
        mock_api.return_value = MockTwitterAPI()

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

import datetime
import dateutil
import os
import unittest

import simplejson
import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)

NEW_USERS_RESPONSE_PAGES = [
    {'json': [{'created_at': '2017-07-01'},
              {'created_at': '2017-07-01'},
              {'created_at': '2017-06-28'}], 'status_code': 200},
    {'json': [{'created_at': '2017-06-27'},
              {'created_at': '2017-06-25'},
              {'created_at': '2017-06-24'}], 'status_code': 200},
    {'json': [], 'status_code': 200},
]

ACTIVE_USERS_RESPONSE = [
    {'json': [{'last_seen_age': '3m'},
              {'last_seen_age': '45h'},
              {'last_seen_age': '2h'}], 'status_code': 200},
    {'json': [{'last_seen_age': '3h'},
              {'last_seen_age': '1d'},
              {'last_seen_age': '25d'}], 'status_code': 200},
    {'json': [], 'status_code': 200},
]

NEW_TOPICS_RESPONSE_PAGES = [
    {'json': {'topic_list': {'topics': [{'created_at': '2017-07-01'},
                                        {'created_at': '2017-07-01'},
                                        {'created_at': '2017-06-28'}]}},
     'status_code': 200},
    {'json': {'topic_list': {'topics': [{'created_at': '2017-06-27'},
                                        {'created_at': '2017-06-25'},
                                        {'created_at': '2017-06-24'}]}},
     'status_code': 200},
    {'json': {'topic_list': {'topics': []}}, 'status_code': 200}
]


class TestDiscourseProcessor(unittest.TestCase):

    @requests_mock.Mocker()
    def test_add_discourse_resource_no_latest(self, m):
        '''No latest data in forums table, so populate with historical data
        where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/admin/users/list/new.json',
              NEW_USERS_RESPONSE_PAGES)
        m.get('https://discourse.example.com/admin/users/list/active.json',
              ACTIVE_USERS_RESPONSE)
        m.get('https://discourse.example.com/latest.json',
              NEW_TOPICS_RESPONSE_PAGES)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': 'discourse.example.com'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'discourse-example-com'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['domain', 'source', 'date', 'new_users',
                               'new_topics', 'active_users']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(list(spew_res_iter_contents)) == 1
        rows = list(spew_res_iter_contents)[0]
        # six days of data
        assert len(rows) == 6
        # two new users for first row
        assert rows[0] == {
            'new_users': 2,
            'new_topics': 2,
            'date': dateutil.parser.parse('2017-07-01').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        # four active users for today
        assert rows[5] == {
            'active_users': 4,
            'new_users': 0,
            'new_topics': 0,
            'date': datetime.date.today(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }

    @requests_mock.Mocker()
    def test_add_discourse_resource_with_latest(self, m):
        '''Latest data in forums table, so populate with historical data upto
        latest where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/admin/users/list/new.json',
              NEW_USERS_RESPONSE_PAGES)
        m.get('https://discourse.example.com/admin/users/list/active.json',
              ACTIVE_USERS_RESPONSE)
        m.get('https://discourse.example.com/latest.json',
              NEW_TOPICS_RESPONSE_PAGES)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'latest-project-entries',
                'schema': {
                    'fields': []
                }
            }]
        }
        params = {
            'domain': 'discourse.example.com'
        }

        def latest_entries_res():
            yield {
                    'active_users': 5,
                    'domain': 'discourse.example.com',
                    'new_users': 1,
                    'date': dateutil.parser.parse('2017-06-28').date(),
                    'source': 'discourse'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_resource.py')

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
        assert dp_resources[1]['name'] == 'discourse-example-com'

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(list(spew_res_iter_contents)) == 2
        rows = list(spew_res_iter_contents)[1]
        # six days of data
        assert len(rows) == 3
        # two new users for first row
        assert rows[0] == {
            'new_users': 2,
            'new_topics': 2,
            'date': dateutil.parser.parse('2017-07-01').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        # this was the row retrieved from latest-project-entries, retain
        # active_users value
        assert rows[1] == {
            'new_users': 1,
            'new_topics': 1,
            'active_users': 5,
            'date': dateutil.parser.parse('2017-06-28').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        # four active users for today
        assert rows[2] == {
            'active_users': 4,
            'new_users': 0,
            'new_topics': 0,
            'date': datetime.date.today(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }

    @requests_mock.Mocker()
    def test_add_discourse_resource_bad_response(self, m):
        '''Bad status api responses from discourse.'''

        # Mock API responses
        m.get('https://discourse.example.com/admin/users/list/new.json',
              text="bad response", status_code=401)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': 'discourse.example.com'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(ValueError) as cm:
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)
        error_msg = "Error raised for domain:discourse.example.com, Status code:401. Error message: b'bad response'"  # noqa
        self.assertEqual(str(cm.exception), error_msg)

    @requests_mock.Mocker()
    def test_add_discourse_resource_not_json(self, m):
        '''Response isn't json error.'''

        # Mock API responses
        m.get('https://discourse.example.com/admin/users/list/new.json',
              text="bad response", status_code=200)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': 'discourse.example.com'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(ValueError) as cm:
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)
        error_msg = "Expected JSON in response from: https://discourse.example.com/admin/users/list/new.json?api_key=myfakediscoursetoken&page=1"  # noqa
        self.assertEqual(str(cm.exception), error_msg)

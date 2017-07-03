import datetime
import dateutil
import os
import unittest

import simplejson

from freezegun import freeze_time
from freezegun.api import FakeDate
import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)

# Mock responses for mailchimp API requests. Real responses contain more data.
GENERAL_LIST_STATS_RESPONSE = {
    'date_created': '2014-09-20T21:39:55+00:00',
    'stats': {
        'member_count': 30000
    }
}

GROWTH_HISTORY_RESPONSE = {
    'month': '2014-09',
    'existing': 247
}


def _make_list_activity_response(date, count):
    response = {
        'list_id': '32d370a7fc',
        'total_items': count,
        'activity': {}
    }
    response['activity'] = [{
        'day': (date - datetime.timedelta(days=n)).strftime('%Y-%m-%d'),
        'subs': n,
        'unsubs': n + 1,
        'other_adds': n + 2,
        'other_removes': n + 3} for n in range(0, count)]
    return response


CAMPAIGNS_RESPONSE = {
    'campaigns': [
        {'send_time': '2014-10-16T13:34:22+00:00'},
        {'send_time': '2014-10-26T12:34:22+00:00'},
        {'send_time': '2014-09-25T13:34:22+00:00'},
        {'send_time': '2014-09-22T13:34:22+00:00'},
        {'send_time': '2014-09-18T13:34:22+00:00'}
    ],
    'total_items': 5
}


class TestMailChimpProcessor(unittest.TestCase):

    @freeze_time("2014-10-18")
    @requests_mock.Mocker()
    def test_add_mailchimp_resource_no_latest(self, m):
        '''No latest data in email table, so populate with historical data.'''

        # Mock API responses
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456',
              json=GENERAL_LIST_STATS_RESPONSE)
        activity_date = dateutil.parser.parse("2014-10-18").date()
        list_activity_response = _make_list_activity_response(activity_date,
                                                              30)
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456/activity?count=29', # noqa
              json=list_activity_response)
        m.get('https://dc1.api.mailchimp.com/3.0/campaigns/?list_id=123456&since_send_time=2014-09-20', # noqa
              json=CAMPAIGNS_RESPONSE)
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456/growth-history/2014-09', # noqa
              json=GROWTH_HISTORY_RESPONSE)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'list_id': '123456'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_mailchimp_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == '123456'
        field_names = \
            [field['name'] for field in dp_resources[0]['schema']['fields']]
        assert field_names == ['source', 'date', 'list_id', 'subs', 'unsubs',
                               'subscribers', 'campaigns_sent']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(list(spew_res_iter_contents)) == 1
        rows = list(spew_res_iter_contents)[0]
        assert len(rows) == 30
        assert rows[0] == {
            'campaigns_sent': 0,
            'list_id': '123456',
            'subs': 2,
            'subscribers': 30000,
            'unsubs': 4,
            'date': FakeDate(2014, 10, 18),
            'source': 'mailchimp'
        }
        # third one has a campaign sent
        assert rows[2] == {
            'campaigns_sent': 1,
            'list_id': '123456',
            'subs': 6,
            'unsubs': 8,
            'date': FakeDate(2014, 10, 16),
            'source': 'mailchimp'
        }

    @freeze_time("2014-10-18")
    @requests_mock.Mocker()
    def test_add_mailchimp_resource_latest_yesterday(self, m):
        '''Latest data was yesterday, so get today and yesterday only.
        Yesterday's values are now final and will overwrite what's there.'''
        # Mock API responses
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456',
              json=GENERAL_LIST_STATS_RESPONSE)

        activity_date = dateutil.parser.parse("2014-10-18").date()
        list_activity_response = _make_list_activity_response(activity_date, 2)
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456/activity?count=2', # noqa
              json=list_activity_response)
        m.get('https://dc1.api.mailchimp.com/3.0/campaigns/?list_id=123456&since_send_time=2014-10-17', # noqa
              json=CAMPAIGNS_RESPONSE)

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
            'list_id': '123456'
        }

        def latest_entries_res():
            yield {
                    'campaigns_sent': 0,
                    'list_id': '123456',
                    'subs': 1,
                    'subscribers': 29000,
                    'unsubs': 0,
                    'date': FakeDate(2014, 10, 17),
                    'source': 'mailchimp'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_mailchimp_resource.py')

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
        assert dp_resources[1]['name'] == '123456'

        # Asserts for the res_iter
        # two resources in res_iter
        resources = list(spew_res_iter)
        assert len(resources) == 2

        # first resource will look like latest_entries_res
        assert list(resources[0])[0] == next(latest_entries_res())

        # second resource is from the mailchimp processor
        res_rows = resources[1]
        assert len(res_rows) == 2
        assert res_rows[0] == {
            'campaigns_sent': 0,
            'list_id': '123456',
            'subs': 2,
            'subscribers': 30000,
            'unsubs': 4,
            'date': FakeDate(2014, 10, 18),
            'source': 'mailchimp'
        }
        assert res_rows[1] == {
            'campaigns_sent': 0,
            'list_id': '123456',
            'subs': 4,  # subs and unsubs have been updated
            'unsubs': 6,
            'date': FakeDate(2014, 10, 17),
            'source': 'mailchimp',
            'subscribers': 29000  # subscribers number is retained from db
        }

    @requests_mock.Mocker()
    def test_add_mailchimp_resource_not_json(self, m):
        # Mock API responses
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456',
              text="This isn't json")

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'list_id': '123456'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_mailchimp_resource.py')

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
    def test_add_mailchimp_resource_bad_status(self, m):
        bad_response = {
            'detail': 'Hi, there was a problem with your request.'
        }
        # Mock API responses
        m.get('https://dc1.api.mailchimp.com/3.0/lists/123456',
              json=bad_response, status_code=401)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'list_id': '123456'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_mailchimp_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(Exception):
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)

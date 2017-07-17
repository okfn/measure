import dateutil
import os
import unittest

import requests_mock

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)

REPORT_RESPONSE = {
    "report": {
        "data": [
            {"x": "2017-07-05", "y": 1},
            {"x": "2017-07-06", "y": 2},
            {"x": "2017-07-07", "y": 3},
            {"x": "2017-07-09", "y": 4},
            {"x": "2017-07-10", "y": 5},
            {"x": "2017-07-11", "y": 6}
        ]
    }
}

RESTRICTED_REPORT_RESPONSE = {
    "report": {
        "data": [
            {"x": "2017-07-09", "y": 4},
            {"x": "2017-07-10", "y": 5},
            {"x": "2017-07-11", "y": 6}
        ]
    }
}

SITE_RESPONSE = {
    "categories": [
        {
            "id": 1,
            "name": "Top One",
            "slug": "top-one",
            "has_children": True,
        },
        {
            "id": 2,
            "name": "Top Two",
            "slug": "top-two",
            "has_children": True,
        },
        {
            "id": 3,
            "name": "Top Three",
            "slug": "top-three",
            "has_children": False,
        },
        {
            "id": 4,
            "name": "Child One",
            "slug": "child-one",
            "parent_category_id": 1,
            "has_children": False,
        },
        {
            "id": 5,
            "name": "Child Two",
            "slug": "child-two",
            "has_children": False,
            "parent_category_id": 1
        },
        {
            "id": 6,
            "name": "Child Three",
            "slug": "child-three",
            "has_children": False,
            "parent_category_id": 2
        }
    ]
}


class TestDiscourseCategoriesProcessor_NoChildren(unittest.TestCase):

    '''Tests for Discourse Category processor when no child treatment has been
    defined.'''

    @requests_mock.Mocker()
    def test_add_discourse_category_resource_no_latest(self, m):
        '''No latest data in forum-categories table, so populate with
        historical data where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/site.json', json=SITE_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=1',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=1',  # noqa
              json=REPORT_RESPONSE)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': 'discourse.example.com',
            'category': {
                'name': 'top-one'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_category_resource.py')

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
        assert field_names == ['domain', 'category', 'source', 'date',
                               'new_topics', 'new_posts']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(list(spew_res_iter_contents)) == 1
        rows = list(spew_res_iter_contents)[0]
        # six days of data
        assert len(rows) == 6
        assert rows[0] == {
            'new_topics': 1,
            'new_posts': 1,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-05').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        assert rows[len(rows) - 1] == {
            'new_topics': 6,
            'new_posts': 6,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-11').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }

    @requests_mock.Mocker()
    def test_add_discourse_category_resource_with_latest(self, m):
        '''Latest data in forum-categories table, so populate with historical
        data upto latest where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/site.json', json=SITE_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=1',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=1',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)

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
            'domain': 'discourse.example.com',
            'category': {
                'name': 'top-one'
            }
        }

        def latest_entries_res():
            yield {
                    'category': 'top-one',
                    'domain': 'discourse.example.com',
                    'new_posts': 1,
                    'new_topics': 1,
                    'date': dateutil.parser.parse('2017-07-09').date(),
                    'source': 'discourse'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_category_resource.py')

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
        # three days of data
        assert len(rows) == 3
        assert rows[0] == {
            'new_topics': 4,
            'new_posts': 4,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-09').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        assert rows[1] == {
            'new_topics': 5,
            'new_posts': 5,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-10').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }


class TestDiscourseCategoriesProcessor_ChildrenAggregate(unittest.TestCase):

    '''Tests for Discourse Category processor when child treatment is
    `aggregate`.'''

    @requests_mock.Mocker()
    def test_add_discourse_category_resource_no_latest(self, m):
        '''No latest data in forum-categories table, so populate with
        historical data where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/site.json', json=SITE_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=1',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=1',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=4',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=4',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=5',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=5',  # noqa
              json=REPORT_RESPONSE)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': 'discourse.example.com',
            'category': {
                'name': 'top-one',
                'children': 'aggregate'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_category_resource.py')

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
        assert field_names == ['domain', 'category', 'source', 'date',
                               'new_topics', 'new_posts']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(list(spew_res_iter_contents)) == 1
        rows = list(spew_res_iter_contents)[0]
        # six days of data
        assert len(rows) == 6
        assert rows[0] == {
            'new_topics': 3,
            'new_posts': 3,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-05').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        assert rows[len(rows) - 1] == {
            'new_topics': 18,
            'new_posts': 18,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-11').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }

    @requests_mock.Mocker()
    def test_add_discourse_category_resource_with_latest(self, m):
        '''Latest data in forum-categories table, so populate with historical
        data upto latest where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/site.json', json=SITE_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=1',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=1',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=4',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=4',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=5',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=5',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)

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
            'domain': 'discourse.example.com',
            'category': {
                'name': 'top-one',
                'children': 'aggregate'
            }
        }

        def latest_entries_res():
            yield {
                    'category': 'top-one',
                    'domain': 'discourse.example.com',
                    'new_posts': 1,
                    'new_topics': 1,
                    'date': dateutil.parser.parse('2017-07-09').date(),
                    'source': 'discourse'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_category_resource.py')

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
        # three days of data
        assert len(rows) == 3
        assert rows[0] == {
            'new_topics': 12,
            'new_posts': 12,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-09').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }
        assert rows[1] == {
            'new_topics': 15,
            'new_posts': 15,
            'category': 'top-one',
            'date': dateutil.parser.parse('2017-07-10').date(),
            'source': 'discourse',
            'domain': 'discourse.example.com'
        }


class TestDiscourseCategoriesProcessor_ChildrenExpand(unittest.TestCase):

    '''Tests for Discourse Category processor when child treatment is
    `expand`.'''

    @requests_mock.Mocker()
    def test_add_discourse_category_resource_no_latest(self, m):
        '''No latest data in forum-categories table, so populate with
        historical data where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/site.json', json=SITE_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=1',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=1',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=4',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=4',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=5',  # noqa
              json=REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=5',  # noqa
              json=REPORT_RESPONSE)

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'domain': 'discourse.example.com',
            'category': {
                'name': 'top-one',
                'children': 'expand'
            }
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_category_resource.py')

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
        assert field_names == ['domain', 'category', 'source', 'date',
                               'new_topics', 'new_posts']

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(list(spew_res_iter_contents)) == 1
        rows = list(spew_res_iter_contents)[0]
        # six days of data for three categories
        assert len(rows) == 18
        categories = [(r['category'], r['new_posts']) for r in rows]
        for i in range(0, 6):
            assert categories[i] == ('top-one', i + 1)
        for i in range(0, 6):
            assert categories[i + 6] == ('child-one', i + 1)
        for i in range(0, 6):
            assert categories[i + 12] == ('child-two', i + 1)

    @requests_mock.Mocker()
    def test_add_discourse_category_resource_with_latest(self, m):
        '''Latest data in forum-categories table, so populate with historical
        data upto latest where possible.'''

        # Mock API responses
        m.get('https://discourse.example.com/site.json', json=SITE_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=1',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=1',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=4',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=4',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/topics.json?category_id=5',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)
        m.get('https://discourse.example.com/admin/reports/posts.json?category_id=5',  # noqa
              json=RESTRICTED_REPORT_RESPONSE)

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
            'domain': 'discourse.example.com',
            'category': {
                'name': 'top-one',
                'children': 'expand'
            }
        }

        def latest_entries_res():
            yield {
                    'category': 'top-one',
                    'domain': 'discourse.example.com',
                    'new_posts': 1,
                    'new_topics': 1,
                    'date': dateutil.parser.parse('2017-07-09').date(),
                    'source': 'discourse'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir,
                                      'add_discourse_category_resource.py')

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
        # three days of data for three categories
        assert len(rows) == 9
        categories = [(r['category'], r['new_posts']) for r in rows]
        for i in range(0, 3):
            assert categories[i] == ('top-one', i + 4)
        for i in range(0, 3):
            assert categories[i + 3] == ('child-one', i + 4)
        for i in range(0, 3):
            assert categories[i + 6] == ('child-two', i + 4)

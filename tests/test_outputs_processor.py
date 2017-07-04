import os
import mock
import datetime
import dateutil
import unittest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_measure.processors

import logging
log = logging.getLogger(__name__)


class TestMeasureOutputsProcessor(unittest.TestCase):

    @mock.patch('datapackage_pipelines_measure.processors.google_utils.get_authorized_http_object') # noqa
    def test_add_outputs_resource_processor_no_latest(self, mock_http_object):
        '''No latest in db, so populate from gsheet request.'''

        gsheets_response = ({'normallythisis': 'headerinformation'}, b'/*O_o*/\ngoogle.visualization.Query.setResponse({"version":"0.6","reqId":"0","status":"ok","sig":"15329035","table":{"cols":[{"id":"A","label":"Timestamp","type":"datetime","pattern":"dd/MM/yyyy HH:mm:ss"},{"id":"B","label":"Email address","type":"string"},{"id":"C","label":"Type of output","type":"string"},{"id":"D","label":"Title","type":"string"},{"id":"E","label":"For what organisation?","type":"string"},{"id":"F","label":"Date","type":"date","pattern":"dd/MM/yyyy"},{"id":"G","label":"Who did this?","type":"string"},{"id":"H","label":"link (if published)","type":"string"}],"rows":[{"c":[{"v":"Date(2017,5,20,11,10,52)","f":"20/06/2017 11:10:53"},{"v":"example@example.com"},{"v":"Talk given"},{"v":"Frictionless Data talk"},{"v":"The pub"},{"v":"Date(2017,5,20)","f":"20/06/2017"},{"v":"Alfred"},{"v":null}]},{"c":[{"v":"Date(2017,5,21,12,17,42)","f":"21/06/2017 12:17:42"},{"v":"example@example.com"},{"v":"Labs hang out"},{"v":"We hung out"},{"v":"GLHS"},{"v":"Date(2017,5,1)","f":"01/06/2017"},{"v":"Bert and Ernie"},{"v":"google.com"}]},{"c":[{"v":"Date(2017,5,22,8,11,23)","f":"22/06/2017 08:11:24"},{"v":"example@example.com"},{"v":"Tutorial/Guide"},{"v":"A lovely blog post"},{"v":"Dutch Postal Service"},{"v":"Date(2017,5,22)","f":"22/06/2017"},{"v":"Harvey"},{"v":null}]}]}});') # noqa

        mock_http_object.return_value.request.return_value = gsheets_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []  # nothing here
        }
        params = {
            'sheet_id': 'mysheetsid',
            'gid': 'mygid',
            'source_type': 'internal'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_outputs_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # one resource
        resources = list(spew_res_iter)
        assert len(resources) == 1

        # rows in resource
        rows = list(resources)[0]
        assert len(rows) == 3
        # first row asserts
        assert rows[0] == {
            'output_date': datetime.date(2017, 6, 20),
            'output_link': None,
            'output_organization': 'The pub',
            'output_person': 'Alfred',
            'output_title': 'Frictionless Data talk',
            'output_type': 'Talk given',
            'source_email': 'example@example.com',
            'source_id': 'mysheetsid/mygid',
            'source_timestamp': datetime.datetime(2017, 6, 20, 11, 10, 53),
            'source': 'gsheets',
            'source_type': 'internal',
            'output_additional_information': None
        }
        # last row asserts
        assert rows[len(rows)-1] == {
            'output_date': datetime.date(2017, 6, 22),
            'output_link': None,
            'output_organization': 'Dutch Postal Service',
            'output_person': 'Harvey',
            'output_title': 'A lovely blog post',
            'output_type': 'Tutorial/Guide',
            'source_email': 'example@example.com',
            'source_id': 'mysheetsid/mygid',
            'source_timestamp': datetime.datetime(2017, 6, 22, 8, 11, 24),
            'source': 'gsheets',
            'source_type': 'internal',
            'output_additional_information': None
        }

    @mock.patch('datapackage_pipelines_measure.processors.google_utils.get_authorized_http_object') # noqa
    def test_add_outputs_resource_processor_latest_resource(self,
                                                            mock_http_object):
        '''Latest resource is available.'''

        gsheets_response = ({'normallythisis': 'headerinformation'}, b'/*O_o*/\ngoogle.visualization.Query.setResponse({"version":"0.6","reqId":"0","status":"ok","sig":"15329035","table":{"cols":[{"id":"A","label":"Timestamp","type":"datetime","pattern":"dd/MM/yyyy HH:mm:ss"},{"id":"B","label":"Email address","type":"string"},{"id":"C","label":"Type of output","type":"string"},{"id":"D","label":"Title","type":"string"},{"id":"E","label":"For what organisation?","type":"string"},{"id":"F","label":"Date","type":"date","pattern":"dd/MM/yyyy"},{"id":"G","label":"Who did this?","type":"string"},{"id":"H","label":"link (if published)","type":"string"}],"rows":[{"c":[{"v":"Date(2017,5,20,11,10,52)","f":"20/06/2017 11:10:53"},{"v":"example@example.com"},{"v":"Talk given"},{"v":"Frictionless Data talk"},{"v":"The pub"},{"v":"Date(2017,5,20)","f":"20/06/2017"},{"v":"Alfred"},{"v":null}]},{"c":[{"v":"Date(2017,5,21,12,17,42)","f":"21/06/2017 12:17:42"},{"v":"example@example.com"},{"v":"Labs hang out"},{"v":"We hung out"},{"v":"GLHS"},{"v":"Date(2017,5,1)","f":"01/06/2017"},{"v":"Bert and Ernie"},{"v":"google.com"}]},{"c":[{"v":"Date(2017,5,22,8,11,23)","f":"22/06/2017 08:11:24"},{"v":"example@example.com"},{"v":"Tutorial/Guide"},{"v":"A lovely blog post"},{"v":"Dutch Postal Service"},{"v":"Date(2017,5,22)","f":"22/06/2017"},{"v":"Harvey"},{"v":null}]}]}});') # noqa

        mock_http_object.return_value.request.return_value = gsheets_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'latest-project-entries',
                'schema': {
                    'fields': [
                        {'name': 'output_date', 'type': 'date'},
                        {'name': 'output_link', 'type': 'string'},
                        {'name': 'output_organization', 'type': 'string'},
                        {'name': 'output_person', 'type': 'string'},
                        {'name': 'output_title', 'type': 'string'},
                        {'name': 'output_type', 'type': 'string'},
                        {'name': 'source_email', 'type': 'string'},
                        {'name': 'source_id', 'type': 'string'},
                        {'name': 'source_timestamp', 'type': 'datetime'},
                        {'name': 'source', 'type': 'string'}
                    ]
                }
            }]
        }
        params = {
            'sheet_id': 'mysheetsid',
            'gid': 'mygid',
            'source_type': 'internal'
        }

        now = datetime.datetime.now()

        def latest_entries_res():
            yield {
                    'output_date': dateutil.parser.parse('2017-05-14').date(),
                    'output_link': 'example.com',
                    'output_organization': 'org',
                    'output_person': 'person',
                    'output_title': 'My Output',
                    'output_type': 'Blog post',
                    'source_email': 'example@example.com',
                    'source_id': 'mysheetsid/mygid',
                    'source_timestamp': now - datetime.timedelta(days=5),
                    'source': 'gsheets',
                    'source_type': 'internal'
                }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_outputs_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage,
                                            iter([latest_entries_res()])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # two resources
        resources = list(spew_res_iter)
        assert len(resources) == 2

        # first resource will look like latest_entries_res
        assert list(resources[0])[0] == next(latest_entries_res())

        # rows in resource
        rows = list(resources)[1]
        assert len(rows) == 3
        # first row asserts
        assert rows[0] == {
            'output_date': datetime.date(2017, 6, 20),
            'output_link': None,
            'output_organization': 'The pub',
            'output_person': 'Alfred',
            'output_title': 'Frictionless Data talk',
            'output_type': 'Talk given',
            'output_additional_information': None,
            'source_email': 'example@example.com',
            'source_id': 'mysheetsid/mygid',
            'source_timestamp': datetime.datetime(2017, 6, 20, 11, 10, 53),
            'source': 'gsheets',
            'source_type': 'internal'
        }
        # last row asserts
        assert rows[len(rows)-1] == {
            'output_date': datetime.date(2017, 6, 22),
            'output_link': None,
            'output_organization': 'Dutch Postal Service',
            'output_person': 'Harvey',
            'output_title': 'A lovely blog post',
            'output_type': 'Tutorial/Guide',
            'output_additional_information': None,
            'source_email': 'example@example.com',
            'source_id': 'mysheetsid/mygid',
            'source_timestamp': datetime.datetime(2017, 6, 22, 8, 11, 24),
            'source': 'gsheets',
            'source_type': 'internal'
        }

    @mock.patch('datapackage_pipelines_measure.processors.google_utils.get_authorized_http_object') # noqa
    def test_add_outputs_resource_processor_no_row_returned(self,
                                                            mock_http_object):
        '''No rows returned from gsheets response.'''

        # Empty rows object in response
        gsheets_response = ({'normallythisis': 'headerinformation'}, b'/*O_o*/\ngoogle.visualization.Query.setResponse({"version":"0.6","reqId":"0","status":"ok","sig":"341768161","table":{"cols":[{"id":"A","label":"Timestamp","type":"datetime","pattern":"dd/MM/yyyy HH:mm:ss"},{"id":"B","label":"Email address","type":"string"},{"id":"C","label":"Type of output","type":"string"},{"id":"D","label":"Title","type":"string"},{"id":"E","label":"For what organisation?","type":"string"},{"id":"F","label":"Date","type":"date","pattern":"dd/MM/yyyy"},{"id":"G","label":"Who did this?","type":"string"},{"id":"H","label":"link (if published)","type":"string"}],"rows":[]}});') # noqa

        mock_http_object.return_value.request.return_value = gsheets_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'sheet_id': 'mysheetsid',
            'gid': 'mygid',
            'source_type': 'internal'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_outputs_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # one resource
        resources = list(spew_res_iter)
        assert len(resources) == 1

        # rows in resource
        rows = list(resources)[0]
        assert len(rows) == 0

    @mock.patch('datapackage_pipelines_measure.processors.google_utils.get_authorized_http_object') # noqa
    def test_add_outputs_resource_processor_error_returned(self,
                                                           mock_http_object):
        '''Error returned by gsheets response.'''

        # Empty rows object in response

        # gsheets_response = ({'normallythisis': 'headerinformation'}, b'/*O_o*/\ngoogle.visualization.Query.setResponse({"version":"0.6","reqId":"0","status":"ok","sig":"341768161","table":{"cols":[{"id":"A","label":"Timestamp","type":"datetime","pattern":"dd/MM/yyyy HH:mm:ss"},{"id":"B","label":"Email address","type":"string"},{"id":"C","label":"Type of output","type":"string"},{"id":"D","label":"Title","type":"string"},{"id":"E","label":"For what organisation?","type":"string"},{"id":"F","label":"Date","type":"date","pattern":"dd/MM/yyyy"},{"id":"G","label":"Who did this?","type":"string"},{"id":"H","label":"link (if published)","type":"string"}],"rows":[]}});') # noqa
        gsheets_response = ({'normallythisis': 'headerinformation'}, b'/*O_o*/\ngoogle.visualization.Query.setResponse({"version":"0.6","reqId":"0","status":"error","errors":[{"reason":"invalid_query","message":"INVALID_QUERY","detailed_message":"Invalid query: PARSE_ERROR: Encountered \\u00221487933109\\u0022 at line 1, column 1.\\nWas expecting one of:\\n    \\u003cEOF\\u003e \\n    \\u0022select\\u0022 ...\\n    \\u0022where\\u0022 ...\\n    \\u0022group\\u0022 ...\\n    \\u0022pivot\\u0022 ...\\n    \\u0022order\\u0022 ...\\n    \\u0022skipping\\u0022 ...\\n    \\u0022limit\\u0022 ...\\n    \\u0022offset\\u0022 ...\\n    \\u0022label\\u0022 ...\\n    \\u0022format\\u0022 ...\\n    \\u0022options\\u0022 ...\\n    "}]});') # noqa

        mock_http_object.return_value.request.return_value = gsheets_response

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'sheet_id': 'mysheetsid',
            'gid': 'mygid',
            'source_type': 'internal'
        }

        # Path to the processor we want to test
        processor_dir = \
            os.path.dirname(datapackage_pipelines_measure.processors.__file__)
        processor_path = os.path.join(processor_dir, 'add_outputs_resource.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, iter([])))

        # Trigger the processor with our mock `ingest` will return an exception
        with self.assertRaises(ValueError):
            spew_res_iter = spew_args[1]
            # attempt access to spew_res_iter raises exception
            list(spew_res_iter)

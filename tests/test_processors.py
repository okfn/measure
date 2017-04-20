# -*- coding: utf-8 -*-

import os
import re
import json
import datetime

from datapackage_pipelines.utilities.lib_test_helpers import (
    ProcessorFixtureTestsBase,
    rejsonize
)

import logging
log = logging.getLogger(__name__)

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
ENV = os.environ.copy()
ENV['PYTHONPATH'] = ROOT_PATH


class MeasureProcessorsFixturesTest(ProcessorFixtureTestsBase):

    def _get_procesor_env(self):
        return ENV

    def _get_processor_file(self, processor):
        processor = processor.replace('.', '/')
        return os.path.join(ROOT_PATH,
                            'datapackage_pipelines_measure',
                            'processors',
                            processor.strip() + '.py')

    @staticmethod
    def _get_first_line(data):
        '''Return the first line of `data` as a python object.'''
        if len(data) > 0:
            data = data[0]
            data = data.split('\n')
            actual = data[0]
            rj_actual = rejsonize(actual)
            return json.loads(rj_actual)


for filename, testfunc in MeasureProcessorsFixturesTest(
                            os.path.join(os.path.dirname(__file__), 'fixtures')
                            ).get_tests():
    globals()['test_processors_%s' % filename] = testfunc


class MeasureProcessorsFixturesTest_UUID(MeasureProcessorsFixturesTest):

    def test_fixture(self, output, dp_out, *args):
        """Test `id` is in output data."""
        (actual_dp, *actual_data) = output.split('\n\n', 1)
        actual_json = self._get_first_line(actual_data)

        assert actual_dp == dp_out, \
            "unexpected value for output datapackage: {}".format(actual_dp)

        assert 'id' in actual_json
        uuid_regexp = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") # noqa
        assert uuid_regexp.match(actual_json['id']), \
            "id must match uuid regexp"


for filename, testfunc in MeasureProcessorsFixturesTest_UUID(
                    os.path.join(os.path.dirname(__file__), 'fixtures_uuid')
                    ).get_tests():
    globals()['test_processors_%s' % filename] = testfunc


class MeasureProcessorsFixturesTest_Timestamp(MeasureProcessorsFixturesTest):

    def test_fixture(self, output, dp_out, *args):
        """Test `timestamp` is in the output data."""
        (actual_dp, *actual_data) = output.split('\n\n', 1)
        actual_json = self._get_first_line(actual_data)

        assert actual_dp == dp_out, \
            "unexpected value for output datapackage: {}".format(actual_dp)

        assert 'timestamp' in actual_json
        try:
            datetime.datetime.strptime(actual_json['timestamp'],
                                       '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            assert False, \
                "Timestamp must be a datetime in the correct format"


for filename, testfunc in MeasureProcessorsFixturesTest_Timestamp(
                    os.path.join(os.path.dirname(__file__),
                                 'fixtures_timestamp')).get_tests():
    globals()['test_processors_%s' % filename] = testfunc

# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import datetime
import subprocess

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
    def _get_first_data(processor, parameters, data_in,
                        dp_out, data_out, env):
        '''Returns the first line of data_out as a python object.'''
        process = subprocess.run([sys.executable, processor, '1',
                                  parameters, 'False', ''],
                                 input=data_in,
                                 stdout=subprocess.PIPE,
                                 env=env)
        output = process.stdout.decode('utf8')
        (actual_dp, *actual_data) = output.split('\n\n', 1)
        assert actual_dp == dp_out, \
            "unexpected value for output datapackage: {}".format(actual_dp)
        if len(actual_data) > 0:
            actual_data = actual_data[0]
            actual_data = actual_data.split('\n')
            actual = actual_data[0]
            rj_actual = rejsonize(actual)
            return json.loads(rj_actual)


for filename, testfunc in MeasureProcessorsFixturesTest(
                            os.path.join(os.path.dirname(__file__), 'fixtures')
                            ).get_tests():
    globals()['test_processors_%s' % filename] = testfunc


class MeasureProcessorsFixturesTest_UUID(MeasureProcessorsFixturesTest):

    @staticmethod
    def test_single_fixture(*args):
        """Test `id` is in output data."""
        actual_json = MeasureProcessorsFixturesTest_UUID._get_first_data(*args)
        assert 'id' in actual_json
        uuid_regexp = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") # noqa
        assert uuid_regexp.match(actual_json['id']), \
            "id must match uuid regexp"


for filename, testfunc in MeasureProcessorsFixturesTest_UUID(
                    os.path.join(os.path.dirname(__file__), 'fixtures_uuid')
                    ).get_tests():
    globals()['test_processors_%s' % filename] = testfunc


class MeasureProcessorsFixturesTest_Timestamp(MeasureProcessorsFixturesTest):

    @staticmethod
    def test_single_fixture(*args):
        """Test `timestamp` is in the output data."""
        actual_json = \
            MeasureProcessorsFixturesTest_Timestamp._get_first_data(*args)
        assert 'timestamp' in actual_json
        try:
            datetime.datetime.strptime(actual_json['timestamp'],
                                       '%d-%m-%Y %H:%M:%S')
        except ValueError:
            assert False, \
                "Timestamp must be a datetime in the correct format"


for filename, testfunc in MeasureProcessorsFixturesTest_Timestamp(
                    os.path.join(os.path.dirname(__file__),
                                 'fixtures_timestamp')).get_tests():
    globals()['test_processors_%s' % filename] = testfunc

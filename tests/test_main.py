# -*- coding: utf-8 -*-

import datapackage_pipelines_measure


def test_what_do_we_want():
    '''Tests that what we want for open data is correct.'''
    assert 'data raw' in datapackage_pipelines_measure.what_we_want()

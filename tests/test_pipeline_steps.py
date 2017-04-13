import os
import pkgutil
import unittest
import inspect

from datapackage_pipelines_measure import pipeline_steps


class TestPipelineSteps(unittest.TestCase):

    '''Test that all submodules of the pipeline_steps module contain the
    expected properties and functions.'''

    def test_modules_contain_expected_items(self):
        pkgpath = os.path.dirname(pipeline_steps.__file__)

        pipeline_modules = [getattr(pipeline_steps, name) for _, name, _
                            in pkgutil.iter_modules([pkgpath])]

        for module in pipeline_modules:
            assert module.label, 'pipeline module must have a label property'
            assert isinstance(module.label, str), 'label must be a string'

            assert module.add_steps, 'pipeline module must have an ' \
                'add_steps function'
            assert callable(module.add_steps), 'add_steps must be callable'
            sig = inspect.signature(module.add_steps)
            sig_args = [a for a in sig.parameters]
            assert sig_args == ['steps', 'pipeline_id']

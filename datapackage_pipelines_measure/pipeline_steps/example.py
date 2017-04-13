'''An example pipeline steps module for testing.'''

import os


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..')


label = 'example'


def add_steps(steps: list, pipeline_id: str) -> list:
    return steps + [
        ('add_metadata', {
            'foo': 'bar'
        })
    ]

import os

from ..config import settings

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..', '..')

label = 'social-media'


def add_steps(steps: list, pipeline_id: str) -> list:
    return steps + [
        ('add_metadata', {
            'github': settings.GITHUB_API_BASE_URL
        }),
        ('add_resource', {
            'name': 'test_resource',
            'url': 'https://docs.google.com/spreadsheets/d/' +
            '1vbhTuMDNCmxdo2rPkkya9v6X1f9eyqvSGsY5YcxlcLk/' +
            'edit#gid=0'
        }),
        ('stream_remote_resources', {}),
        ('measure.capitalise', {}),
        ('dump.to_path', {
            'out-path':
                '{}/downloads/{}'.format(ROOT_PATH, pipeline_id)
        })
    ]

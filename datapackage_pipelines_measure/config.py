import os

try:
    from .settings_dev import *  # noqa
except ImportError:
    pass


NAMESPACE = 'MEASURE_'


class Config(object):
    '''Access global configuration.'''

    def __init__(self, env):
        for e_tuple in env:
            setattr(self, e_tuple[0], e_tuple[1])

    def __iter__(self):
        return ((k, v) for k, v in self.__dict__.items()
                if not k.startswith('__') and
                not callable(getattr(self, k)))


settings = Config([(k.replace(NAMESPACE, ''), v)
                   for k, v in os.environ.items()
                   if k.startswith(NAMESPACE)])

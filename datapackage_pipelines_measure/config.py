from distutils.util import strtobool
import os
import collections
from dotenv import load_dotenv, find_dotenv


NAMESPACE = 'MEASURE_'


class Config(collections.MutableMapping):
    '''A dict-like object to access global configuration.'''

    def _bool_transform(self, s):
        '''Transforms string into bools. If `s` can be a native bool, it is
        transformed, otherwise, `s` is returned unaltered.'''
        try:
            s = strtobool(s)
        except ValueError:
            pass
        return s

    def __init__(self, env_items, namespace):
        self.store = dict()

        # Only use env values for keys starting with `namespace` and transform
        # boolean-like values to bools.
        env_items = {k.replace(namespace, ''): self._bool_transform(v)
                     for k, v in env_items if k.startswith(namespace)}

        self.store.update(env_items)

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]


load_dotenv(find_dotenv())
settings = Config(os.environ.items(), NAMESPACE)

import collections

from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()

resource_name = parameters['name']
resources_matcher = ResourceMatcher(resource_name)


datapackage['resources'] = [res for res in datapackage['resources']
                            if not resources_matcher.match(res['name'])]


def process_resources(res_iter_):

    while True:
        resource_ = next(res_iter_)
        if resources_matcher.match(resource_.spec['name']):
            # This is the one we're deleting, empty the iterator.
            collections.deque(resource_, maxlen=0)
        else:
            yield resource_


spew(datapackage, process_resources(res_iter))

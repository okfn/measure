import json

from datapackage_pipelines.wrapper import spew, ingest

import logging
log = logging.getLogger(__name__)

parameters, datapackage, res_iter = ingest()


def show_sample(res):
    for i, row in enumerate(res):
        if i < 10:
            logging.info('#%s: %r', i, row)
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield show_sample(first)

    for res in res_iter_:
        yield res


log.info(json.dumps(datapackage, indent=2))

spew(datapackage, process_resources(res_iter))

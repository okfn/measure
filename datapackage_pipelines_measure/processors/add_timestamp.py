import datetime

from datapackage_pipelines.wrapper import process

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)


def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].append({
      'name': 'timestamp',
      'type': 'datetime',
      'format': '{}'.format(settings.TIMESTAMP_DEFAULT_FORMAT)
    })
    return datapackage


def process_row(row, *args):
    now = datetime.datetime.now().strftime(settings.TIMESTAMP_DEFAULT_FORMAT)
    row['timestamp'] = now
    return row


process(modify_datapackage=modify_datapackage, process_row=process_row)

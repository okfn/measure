'''This processor adds a timestamp for the datetime of collection.

Note: the collection date may not reflect the date the data collected pertains
to, especially for historic data.
'''

import datetime

from datapackage_pipelines.wrapper import process

from datapackage_pipelines_measure.config import settings


NOW = datetime.datetime.now().strftime(settings.TIMESTAMP_DEFAULT_FORMAT)


def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].append({
      'name': 'timestamp',
      'type': 'datetime',
      'format': '{}'.format(settings.TIMESTAMP_DEFAULT_FORMAT)
    })
    return datapackage


def process_row(row, *args):
    row['timestamp'] = NOW
    return row


process(modify_datapackage=modify_datapackage, process_row=process_row)

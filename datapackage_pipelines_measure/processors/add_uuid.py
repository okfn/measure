import uuid

from datapackage_pipelines.wrapper import process

import logging
log = logging.getLogger(__name__)


def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].append({
      'name': 'id',
      'type': 'string',
      'format': 'uuid',
      'constraints': {
        'required': True,
        'unique': True
      }
    })
    datapackage['resources'][0]['schema']['primaryKey'] = 'id'
    return datapackage


def process_row(row, *args):
    row['id'] = str(uuid.uuid4())
    return row


process(modify_datapackage=modify_datapackage, process_row=process_row)

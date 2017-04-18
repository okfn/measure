from datapackage_pipelines.wrapper import process

import logging
log = logging.getLogger(__name__)


def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].append({
      'name': 'project_id',
      'type': 'string',
      'constraints': {
        'required': True
      }
    })
    return datapackage


def process_row(row, row_index,
                resource_descriptor, resource_index,
                parameters, stats):
    row['project_id'] = parameters['name']
    return row


process(modify_datapackage=modify_datapackage, process_row=process_row)

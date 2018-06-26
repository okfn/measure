import itertools

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text

import logging
log = logging.getLogger(__name__)


parameters, datapackage, res_iter = ingest()

table = parameters['table']
engine = parameters['engine']
resource_name = parameters['resource-name']
distinct_on = ', '.join(parameters['distinct_on'])
sort_date_key = parameters.get('sort_date_key', 'date')

Base = automap_base()
engine = create_engine(engine)
# Reflect the tables
Base.prepare(engine, reflect=True)

try:
    Table = Base.classes[table]
except KeyError:
    # No table in database, spew nothing extra
    spew(datapackage, res_iter)
else:
    session = Session(engine)

    s = text(
        "SELECT DISTINCT ON ({0}) *"
        "FROM {1} "
        "ORDER BY {0}, {2} DESC"
        .format(distinct_on, table, sort_date_key)
    )

    results = session.query(Table).from_statement(s).all()

    resource_content = []

    for result in results:
        row = dict((col, getattr(result, col))
                   for col in result.__table__.columns.keys())
        resource_content.append(row)

    resource = {
        'name': resource_name,
        'path': 'data/{}.csv'.format(resource_name),
        PROP_STREAMING: True
    }

    # Temporarily set all types to string.
    if len(resource_content):
        resource['schema'] = {
            'fields': [{'name': h, 'type': 'string'}
                       for h in resource_content[0].keys()]}
    else:
        resource['schema'] = {
            'fields': [{'name': 'empty', 'type': 'string'}]
        }

    datapackage['resources'].append(resource)

    spew(datapackage, itertools.chain(res_iter, [resource_content]))

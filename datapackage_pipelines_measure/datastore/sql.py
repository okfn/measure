from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, desc

from datapackage_pipelines_measure.config import settings

import logging
log = logging.getLogger(__name__)


class SQLDatastore():

    def __init__(self):
        self.Base = automap_base()
        self.engine = create_engine(settings.DB_ENGINE)
        # Reflect the tables
        self.Base.prepare(self.engine, reflect=True)

    def get_latest_from_table(self, filter, table):
        '''
        Get the most recent row from a table with the passed filter.

        Return result as a dict (or None).
        '''
        try:
            Table = self.Base.classes[table]
        except KeyError:
            # No table in database
            return None

        session = Session(self.engine)
        row = session.query(Table) \
            .order_by(desc(Table.timestamp)) \
            .filter_by(**filter) \
            .first()

        if row is None:
            return None

        # Return row's columns as dict
        return dict((col, getattr(row, col))
                    for col in row.__table__.columns.keys())

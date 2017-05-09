from .sql import SQLDatastore


def get_datastore():
    datastore = SQLDatastore()
    return datastore

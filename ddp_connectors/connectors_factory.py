from cmr_connectors_lib.database_connectors.postgres_connector import PostgresConnector
from cmr_connectors_lib.database_connectors.sql_server_connector import SqlServerConnector
from cmr_connectors_lib.database_connectors.informix_connector import InformixConnector


class ConnectorFactory():
    """Helper class that provides a standard way to create a Data Checker using factory method"""
    
    def __init__(self):
        pass
    
    def create_connector(self, connector_type, connector_settings):

        if connector_type == 'sqlserver':
            connector = SqlServerConnector(connector_settings["host"], connector_settings["user"],
                                           connector_settings["password"],connector_settings["port"],
                                           connector_settings["database"])
            return connector

        elif connector_type == 'postgres':
            connector = PostgresConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"], connector_settings.get("schema", 'public'))
            return connector

        elif connector_type == 'informix':
            connector = InformixConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"], connector_settings["protocol"], connector_settings["locale"])
            return connector
    
    
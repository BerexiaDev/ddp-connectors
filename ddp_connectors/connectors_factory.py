class ConnectorFactory():
    """Helper class that provides a standard way to create a Data Checker using factory method"""
    
    def __init__(self):
        pass
    
    def create_connector(self, connector_type, connector_settings):

        if connector_type == 'sftp':
            from ddp_connectors.file_connectors.sftp_connector import SftpConnector
            return SftpConnector.from_settings(connector_settings)

        if connector_type == 'sqlserver':
            from ddp_connectors.database_connectors.sql_server_connector import SqlServerConnector
            connector = SqlServerConnector(connector_settings["host"], connector_settings["user"],
                                           connector_settings["password"],connector_settings["port"],
                                           connector_settings["database"])
            return connector

        elif connector_type == 'postgres':
            from ddp_connectors.database_connectors.postgres_connector import PostgresConnector
            connector = PostgresConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"], connector_settings.get("schema", 'public'))
            return connector

        elif connector_type == 'informix':
            from ddp_connectors.database_connectors.informix_connector import InformixConnector
            connector = InformixConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"], connector_settings["protocol"], connector_settings["locale"])
            return connector
        
        
        elif connector_type == 'oracle':
            from ddp_connectors.database_connectors.oracle_connector import OracleConnector
            connector = OracleConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"], connector_settings.get("schema"))
            return connector

    
        elif connector_type == 'mongo':
            from ddp_connectors.database_connectors.mongo_connector import MongoConnector
            connector = MongoConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"])
            return connector
        elif connector_type == 'mysql':
            from ddp_connectors.database_connectors.mysql_connector import MySQLConnector
            connector = MySQLConnector(connector_settings["host"], connector_settings["user"],
                                       connector_settings["password"], connector_settings["port"],
                                       connector_settings["database"])
            return connector

        elif connector_type == 'db2i':
            from ddp_connectors.database_connectors.db2i_connector import Db2iConnector
            connector = Db2iConnector(connector_settings["host"], connector_settings["user"],
                                      connector_settings["password"], connector_settings["port"],
                                      connector_settings["database"], connector_settings.get("schema"))
            return connector

        raise ValueError("Unsupported connector type: {}".format(connector_type))

    

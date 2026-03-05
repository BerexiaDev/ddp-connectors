#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
from typing import List, Dict
import pyodbc
from loguru import logger
from pyodbc import Cursor

from .sql_connector import SqlConnector
from .sql_connector_utils import cast_informix_to_typescript_types, cast_informix_to_postgresql_type, safe_convert_to_string


class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, protocol, locale):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        self.locale = locale
        self.driver_path = "app/main/drivers/ddifcl28.so"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def get_connection(self):
        conn_str = (
            f"DRIVER={self.driver_path};"
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL={self.protocol};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"CLIENT_LOCALE={self.locale};"   # Explicitly set client locale
            f"DB_LOCALE={self.locale};"       # Explicitly set database locale
            "LoginTimeout=10;"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        # Set decoding to ISO-8859-1 (en_US.819) for both SQL_CHAR and SQL_WCHAR
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')  # ISO-8859-1 = Latin-1
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')  # In case of wide chars
        return conn
    
    
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        query = f'SELECT SKIP {offset} FIRST {limit} * FROM {table_name};'
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        connection = self.get_connection()
        cursor = connection.execute(query)
        try:
            rows = cursor.fetchall()
            column_names = [col[0] for col in cursor.description]
            return [
                {col: safe_convert_to_string(row[i]) for i, col in enumerate(column_names)}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error extracting batch from {table_name}: {str(e)}")
            return []
        finally:
            cursor.close()
            connection.close()



    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        try:
            query = f'SELECT SKIP {offset} FIRST {limit} * FROM {table_name}'
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []

    def stream_batch(self, cursor: pyodbc.Cursor, table_name: str, batch_size: int = 10_000,):
        """
        Full-sync streaming for Informix (no SKIP / OFFSET).
        Uses a single SELECT and fetchmany to avoid performance degradation.
        Suitable for truncate + reload scenarios.
        """
        try:
            cursor.arraysize = batch_size
            logger.info(f"Start streaming Informix table {table_name} with batch_size={batch_size}")

            cursor.execute(f"SELECT * FROM {table_name}")

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

            logger.info(f"Finished streaming Informix table {table_name}")

        except Exception as exc:
            logger.error(f"Error streaming batch from Informix table {table_name}: {exc}")
            return

    def get_connection_tables(self):
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT tabname FROM systables WHERE tabtype = 'T' AND tabname NOT LIKE 'sys%'")
            tables = [row.tabname for row in cursor.fetchall()]
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def get_connection_columns(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                SELECT colname, coltype 
                FROM syscolumns 
                WHERE tabid = (SELECT tabid FROM systables WHERE tabname = '{table_name}')
            """)
            rows = cursor.fetchall()

            columns = [{'name': row.colname, 'type': cast_informix_to_typescript_types(row.coltype)} for row in rows]
            return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    
    def count_table_rows(self, table_name: str) -> int:
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            count_result = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            total_count = int(count_result[0]) if count_result else 0
            return total_count
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            raise 0
        finally:
            cursor.close()
            connection.close()
    
    
    def get_database_schema(self) -> Dict[str, Dict]:
        try:
            # TODO: Use the connector to get the schema based on the connector type
            logger.debug("Getting database schema for Informix using pyodbc")
            tables = {}
            cursor = self.get_connection().cursor()
            
            # First, get the current database
            try:
                cursor.execute("SELECT TRIM(DBINFO('dbname')) FROM systables WHERE tabid = 1")
                current_db = cursor.fetchone()[0]
                logger.debug(f"Current database: {current_db}")
            except Exception as db_err:
                logger.warning(f"Could not get current database: {db_err}")
                current_db = None
            
            # Get all user tables from Informix with more inclusive query
            table_query = """
                SELECT DISTINCT
                    t.tabname,
                    t.owner
                FROM systables t
                WHERE t.tabtype = 'T'
                AND t.tabid >= 100  -- User tables typically start from 100
            """
            logger.debug(f"Executing table query: {table_query}")
            cursor.execute(table_query)
            table_rows = cursor.fetchall()
            
            logger.debug(f"Found {len(table_rows)} tables")
            
            for table_row in table_rows:
                table_name = table_row[0].strip()
                owner = table_row[1].strip() if table_row[1] else None
                
                logger.debug(f"Processing table: {table_name}, owner: {owner}")
                
                # Get columns for each table
                column_query = f"""
                    SELECT 
                        c.colname,
                        c.coltype,
                        c.collength
                    FROM syscolumns c
                    JOIN systables t ON c.tabid = t.tabid
                    WHERE t.tabname = '{table_name}'
                    ORDER BY c.colno
                """
                
                try:
                    logger.debug(f"Getting columns for table {table_name}")
                    cursor.execute(column_query)
                    columns = cursor.fetchall()
                    
                    if columns:
                        # Build columns list while logging each column's info
                        column_list = []
                        for col in columns:
                            col_name = col[0].strip()
                            col_type = cast_informix_to_typescript_types(col[1])
                            logger.debug(f"Column {col_name} has type {col[1]} wihch is {col_type}")
                            column_list.append({
                                'name': col_name,
                                'type': col_type,
                                'length': col[2]
                            })
                            
                        table_info = {
                            'name': table_name,
                            'owner': owner,
                            'columns': column_list
                        }
                        
                        tables[table_name] = table_info
                        logger.debug(f"Added table {table_name} with {len(columns)} columns")
                except Exception as col_err:
                    logger.error(f"Error getting columns for table {table_name}: {col_err}")
                    continue
                
            logger.debug(f"Retrieved schema for {len(tables)} tables")
            if len(tables) == 0:
                logger.warning("No tables found in the schema")
                
            return tables
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            raise ValueError(f"Failed to retrieve database schema: {str(e)}")
        
        
    def extract_table_schema(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            query = f'''
                SELECT
                c.colno      AS ordinal_position,
                c.colname,
                c.coltype,
                c.collength,
                CASE
                    WHEN BITAND(c.coltype, 256) = 256 THEN 'NO'
                    ELSE 'YES'
                END                                       AS is_nullable,

                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM   sysconstraints sc
                        JOIN   sysindexes     si ON sc.idxname = si.idxname
                        WHERE  sc.constrtype = 'P'
                          AND  sc.tabid     = c.tabid
                          AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                                OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                                OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                                OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                                OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                                OR c.colno = si.part16)
                    ) THEN 'YES' ELSE 'NO'
                END                                       AS is_primary_key,

                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM   sysconstraints sc
                        JOIN   sysreferences  sr ON sc.constrid = sr.constrid
                        JOIN   sysindexes     si ON sc.idxname  = si.idxname
                        WHERE  sc.constrtype = 'R'
                          AND  sc.tabid      = c.tabid
                          AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                                OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                                OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                                OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                                OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                                OR c.colno = si.part16)
                    ) THEN 'YES' ELSE 'NO'
                END                                       AS is_foreign_key,


                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM   sysindexes si
                        WHERE  si.tabid = c.tabid
                          AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                                OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                                OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                                OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                                OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                                OR c.colno = si.part16)
                    ) THEN 'YES' ELSE 'NO'
                END                                       AS is_index,


                d.default                                 AS default_value
            FROM   syscolumns   c
            JOIN   systables    t ON c.tabid = t.tabid
            LEFT   JOIN sysdefaults d ON c.tabid = d.tabid AND c.colno = d.colno
            WHERE  t.tabname = '{table_name}'
            ORDER  BY c.colno;
            '''
            cursor.execute(query)
            rows = cursor.fetchall()

            columns = [
                {
                    'position': col[0],
                    'name': col[1],
                    'type': cast_informix_to_postgresql_type(col[2]),
                    'length': col[3],
                    'nullable': col[4],
                    'primary_key': col[5],
                    'foreign_key': col[6],
                    "is_index": col[7],
                    'default': col[8]
                }
                for col in rows
            ]
            return columns
        
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ):
        # Build composite key condition and ORDER BY
        pk_match = " AND ".join(f"lt2.{pk} = lt1.{pk}" for pk in primary_keys)
        order_by = ", ".join(primary_keys)

        sql = f"""
            SELECT SKIP ? FIRST ? *
            FROM {log_table} lt1
            WHERE lt1.Date_operation = (
                SELECT MAX(lt2.Date_operation)
                FROM {log_table} lt2
                WHERE {pk_match}
                AND lt2.Date_operation > ?
            )
            AND lt1.Date_operation > ?
            ORDER BY {order_by};
        """

        offset = 0
        while True:
            cursor.execute(sql, (offset, batch_size, since_ts, since_ts))
            rows = cursor.fetchall()
            if not rows:
                break

            col_names = [c[0] for c in cursor.description]
            for tup in rows:
                yield dict(zip(col_names, tup))

            offset += batch_size


    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE / DATETIME column in an Informix table.
        Assumes snake_case identifiers (no spaces), so no quoting.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            sql = f"""
                SELECT MIN({column_name}) AS min_val, MAX({column_name}) AS max_val
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            """
            logger.info(f"Getting min/max for {table_name}.{column_name}")
            cursor.execute(sql)
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cursor.close()
            conn.close()

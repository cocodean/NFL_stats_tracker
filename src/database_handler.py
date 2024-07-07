# -*- coding: utf-8 -*-
import os
from pathlib import Path
import sqlite3
from typing import Union
import typing

class Database_Handler():
    '''
    @brief: used to interact with sqlite3 database
        v1 - push data to database, non-persistant connection
    '''
    
    #-------------------------------------------------------------------------#
    
    def __init__(self):
        pass
    
    #-------------------------------------------------------------------------#
    
    def insert_data(self, 
                  db_path:str, 
                  table_name:str, 
                  monitors:list, 
                  values:list,
                  create_db:bool=False) -> bool:
        data_inserted = False
        
        # get connection to database
        conn = self._connect_to_db(db_path, create_db)
        if conn is None:
            print(f'INFO: Failed to insert data becuase no connection was made to {db_path}')
            return data_inserted
        
        # create the sql query
        # verify table or create table
        
        sql_stmt = f''
        
    
    #-------------------------------------------------------------------------#
    
    def create_table(self, 
                     db_path:str, 
                     table_name:str, 
                     monitors_types_d:dict,
                     create_db:bool=False) -> bool:
        table_created = False
        
        # get connection to database
        conn = self._connect_to_db(db_path, create_db)
        if conn is None:
            print(f'INFO: Failed to create table becuase no connection was made to {db_path}')
            return table_created
        
        # prepare the sql statement
        # "monitor_name monitor_type"
        table_monitors_l = [f'{k} {v}' for k,v in monitors_types_d.items()]
        
        # (m1_name m1_type, m2_name m2_type, ...)
        table_monitors_str = ','.join(table_monitors_l)
        
        sql_stmnt = f'CREATE TABLE {table_name} ({table_monitors_str})'
        print(f'INFO: executing the sql statement\n{sql_stmnt}')
        
        # execute the sql statement
        cursor = conn.cursor()
        try:
            cursor.execute(sql_stmnt)
            table_created = True
        except Exception as e:
            print(f'ERROR: failed to execute sql statement for reason -> {e}')
        
        return table_created
    
    #-------------------------------------------------------------------------#
    
    def _connect_to_db(self, 
                       db_path:str, 
                       create_db:bool=False) -> Union[sqlite3.Connection, None]:
        conn = None
        
        # verify the provided path exists currently and we dont want to create one
        if (not os.path.exists(db_path)) and \
            (not os.path.isfile(db_path) ) and \
            (not create_db):
            print(f'ERROR: the providied database path does not exist, {db_path}')
            return conn
        
        # make the connection
        try:
            conn = sqlite3.connect(db_path)
        except Exception as e:
            print(f'ERRPR: failed to make connection to {db_path} for reason -> {e}')
            conn = None
            
        return conn
    
    #-------------------------------------------------------------------------#
    

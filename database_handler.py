# -*- coding: utf-8 -*-
import os
from pathlib import Path
import sqlite3
import logging
from typing import Union
import typing

class Database_Handler():
    '''
    @brief: used to interact with sqlite3 database
        v1 - push data to database, non-persistant connection
    '''
    
    #-------------------------------------------------------------------------#
    
    def __init__(self, ilogger = None):
        '''
        Default constructor

        Parameters
        ----------
        ilogger : logging.logger, optional
            The logger to use for debug purposes. The default is None.

        Returns
        -------
        None.

        '''
        if ilogger is None:
            logName = os.path.join(os.getcwd(), 'database_handler.log')
            print(f'INFO: saving log to {logName}')
            self._mLogger = logging.getLogger(__name__)
            logging.basicConfig(filename=logName, 
                                encoding='utf-8', 
                                level=logging.DEBUG)
        else:
            self._mLogger = ilogger
    
    #-------------------------------------------------------------------------#
    
    def insert_data(self, 
                  db_path:str, 
                  table_name:str, 
                  data_d:dict, 
                  create_db:bool=False) -> bool:
        '''
        Inserts a dicitonary of data into the table. Works under the assumption
        that the table accepts all the key-value pairs in the dictionary.

        Parameters
        ----------
        db_path : str
            Full path to the sqlite database file.
        table_name : str
            The table name in the database to insert the data.
        data_d : dict
            The key-value pairs to insert into the table.
        create_db : bool, optional
            Create the database, if it does not exist. The default is False.

        Returns
        -------
        bool
            True for data inserted into database table, False otherwise.
        '''
        data_inserted = False
        
        # get connection to database
        conn = self._connect_to_db(db_path, create_db)
        if conn is None:
            self._mLogger.error(f'Failed to insert data becuase no connection was made to {db_path}')
            return data_inserted
        
        #
        # ASSUME table exists since we dont know the column types to create one
        #
        
        # create sql insert statement
        col_names = list()
        col_vals = list()
        for col_name, col_val in data_d.items():
            col_names.append(col_name)
            if isinstance(col_val, str):
                col_vals.append(f'"{col_val}"')
            else:
                col_vals.append(f'{col_val}')
        col_names = ','.join(col_names)
        col_vals = ','.join(col_vals)
        sql_stmnt = f'INSERT INTO {table_name} ({col_names}) VALUES ({col_vals});'
        self._mLogger.info(f'Executing the sql insert statement,\n{sql_stmnt}')
        
        try:
            cursor = conn.cursor()
            result = cursor.execute(sql_stmnt)
            data_inserted = True
        except Exception as e:
            self._mLogger.error(f'Failed to execute sql stament {sql_stmnt} for reason -> {e}')
        
        conn.commit()
        conn.close()
        
        return data_inserted
    
    #-------------------------------------------------------------------------#
    
    def create_table(self, 
                     db_path:str, 
                     table_name:str, 
                     monitors_types_d:dict,
                     create_db:bool=False,
                     conn=None) -> bool:
        '''
        Creates the table and its columns in a sqlite database.

        Parameters
        ----------
        db_path : str
            Full path to the sqlite database file.
        table_name : str
            The database table name to create.
        monitors_types_d : dict
            Dictionary of monitor types as monitor_name -> data_type pairs.
        create_db : bool, optional
            If the database should be created, if not already exist. 
            The default is False.
        conn : sqlite3.Connection, optional
            sqlite3.Connection object to use, if exists. The default is None.

        Returns
        -------
        bool
            True if the table was created, False otherwise.

        '''
        table_created = False
        
        if conn is None:
            # get connection to database
            conn = self._connect_to_db(db_path, create_db)
            if conn is None:
                self._mLogger.error(f'Failed to create table becuase no connection was made to {db_path}')
                return table_created
        
        # prepare the sql statement
        # "monitor_name monitor_type"
        table_monitors_l = [f'{k} {v}' for k,v in monitors_types_d.items()]
        
        # (m1_name m1_type, m2_name m2_type, ...)
        table_monitors_str = ','.join(table_monitors_l)
        
        sql_stmnt = f'CREATE TABLE {table_name} ({table_monitors_str})'
        self._mLogger.info(f'Executing the create table sql statement\n{sql_stmnt}')
        
        # execute the sql statement
        cursor = conn.cursor()
        try:
            cursor.execute(sql_stmnt)
            table_created = True
        except Exception as e:
            self._mLogger.error(f'ailFed to execute sql statement, {sql_stmnt}, for reason -> {e}')
        
        conn.commit()
        conn.close()
        
        return table_created
    
    #-------------------------------------------------------------------------#
    
    def _connect_to_db(self, 
                       db_path:str, 
                       create_db:bool=False) -> Union[sqlite3.Connection, None]:
        '''
        Makes a connection to a sqlite database.

        Parameters
        ----------
        db_path : str
            Full path to the sqlite database file.
        create_db : bool, optional
            If the sqlite database should be created, if not alredy. 
            The default is False.

        Returns
        -------
        conn : sqlite3.Connection
            sqlite3 data database connection if successful, None otherwise.
        '''
        conn = None
        
        # verify the provided path exists currently and we dont want to create one
        if (not os.path.exists(db_path)) and \
            (not os.path.isfile(db_path) ) and \
            (not create_db):
            self._mLogger.error(f'The providied database path does not exist, {db_path}')
            return conn
        
        # make the connection
        try:
            conn = sqlite3.connect(db_path)
        except Exception as e:
            self._mLogger(f'Failed to make connection to {db_path} for reason -> {e}')
            conn = None
            
        return conn
    
    #-------------------------------------------------------------------------#
    

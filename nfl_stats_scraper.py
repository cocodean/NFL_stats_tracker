import pandas as pd
import numpy as np

import bs4
from bs4 import BeautifulSoup
from selenium import webdriver

#import requests
import re
import os
import time

from nfl_stats_scraper_constants import TEAM_ABR_TO_NAME, MAIN_URL, TARGET_GAME_TABLES, SLEEP_GET_SOUP_SEC
from table_to_dataframe_methods import METHOD_CONVERSION_MAP, get_simple_table_df

class NFL_Stats_Scraper:
    
    def __init__(self):
        '''
        Default constructor

        Returns
        -------
        None.
        '''
        print("DEBUG: NFL_Stats_Scraper instance created...")
        
        # setup selenium driver
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        self.__mDriver = webdriver.Chrome(options=options)
        self.__mDriver.implicitly_wait(5)
        
    #-------------------------------------------------------------------------#
    
    def get_rushing_stats(self, year:int, save_csv:bool=False, csv_path:str='.'):
        '''
        Retrieves the player rushing statistics for the year selected

        Parameters
        ----------
        year : int
            The footbal year.
        save_csv : bool, optional
            If the csv should be saved. The default is False.
        csv_path : str
            The path to save the csv file. The default is '.'

        Returns
        -------
        stats : pd.DataFrame
            The pandas data frame for the table found.
        '''
        print(f'INFO: getting rushing stats for the year {year}')
        
        table_id = 'rushing'
        target_url = f'{MAIN_URL}/years/{year}/rushing.htm'
        
        soup = self.get_soup(target_url)
        
        stats = self.__get_table_df__(table_id, soup)
        
        stats = self.__clean_df_names__(stats)
        
        stats['Year'] = year
        
        if save_csv:
            csv_name = f'rushing_stats_{year}'
            rushing_csv_path = os.path.join(csv_path, 'rushing')
            self.__save_to_csv__(stats, csv_name, rushing_csv_path)
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def get_passing_stats(self, year:int, save_csv:bool=False, csv_path:str='.'):
        '''
        Retrieves the player passing statistics for the year selected

        Parameters
        ----------
        year : int
            The footbal year.
        save_csv : bool, optional
            If the csv should be saved. The default is False.
        csv_path : str
            The path to save the csv file. The default is '.'

        Returns
        -------
        stats : pd.DataFrame
            The pandas data frame for the table found.
        '''
        print(f'INFO: getting passing stats for the year {year}')
        
        table_id = 'passing'
        target_url = f'{MAIN_URL}/years/{year}/passing.htm'
        
        soup = self.get_soup(target_url)
        
        stats = self.__get_table_df__(table_id, soup)
        
        stats = self.__clean_df_names__(stats)
        
        stats['Year'] = year
        
        if save_csv:
            csv_name = f'passing_stats_{year}'
            passing_csv_path = os.path.join(csv_path, 'passing')
            self.__save_to_csv__(stats, csv_name, passing_csv_path)
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def get_week_game_stats(self, year:int, week_num:int, save_csv:bool=False,
                            save_path:str='.'):
        '''
        Gets the DataFrames for a week's worth of games for the specified year
        Parameters
        ----------
        year : int
            The year to search statistics for. Limited to [1980, Present_Year]
        week_num : int
            The game week to get statistics for. Limited to 17 before 2021 and
            18 for 2021 and after.

        Returns
        -------
        games_dfs : dictionary
            dictionary of dictionaries of pd.DataFrame
            Ex. games_dfs['game_0']['team_stats'] = pd.DataFrame
        '''
        print(f'INFO: getting game stats for week {week_num} of year {year}')
        
        target_url = f'{MAIN_URL}/years/{year}/week_{week_num}.htm'
        
        soup = self.get_soup(target_url)
        
        # get all the tables for the teams box scores, they hold the hyperlink
        # to the games stats page
        tables = soup.find_all('table')
        print(f'DEBUG: total tables found for webpage is {len(tables)}')
        
        tables = [table for table in tables
                  if 'teams' in table.get('class')]
        
        print(f'DEBUG: Total game tables found is {len(tables)}')
        
        
        #
        # Get the link to every game's boxscore for the week
        #
        links_map = dict()
        
        for idx, table in enumerate(tables):
            links = table.find_all('a')     # look for all the anchor tags
            for link in links:
                if 'boxscore' in link.get('href'):
                    links_map[idx] = MAIN_URL + link.get('href')
                    print(f'DEBUG: {idx} -> Adding the following link {links_map[idx]}')
                    break
        
        #
        # Go to the page for each game and get the relevant tables to parse
        #
        games_dfs = dict()
        for k,v in links_map.items():
            game_key = f'game_{k}'
            games_dfs[game_key] = dict()
            
            soup = self.get_soup(v)
            
            teams_playing = soup.find('div', role='main').h1.text
            games_dfs[game_key]['teams_playing'] = teams_playing
            
            tables = {table.get('id'): table for table in soup.find_all('table')
                      if table.get('id') in TARGET_GAME_TABLES}
            print(f'DEBUG: {k}: {len(tables)}')
            
            # process the tables
            for tableName, tableData in tables.items():
                df = METHOD_CONVERSION_MAP[tableName](tableData)
                
                games_dfs[game_key][tableName] = df
                
                if save_csv:
                    print(f'DEBUG: Saving the dataframe for week {week_num} game {game_key} {tableName}')
                    csvName = f'{tableName}.csv'
                    csvPath = os.path.join(save_path, str(year), f'week_{week_num}', game_key)
                    self.__save_to_csv__(df, csvName, csvPath)
            
            # wait to try a new web page
            time.sleep(10)
        
        return games_dfs
    
    #-------------------------------------------------------------------------#

    def get_soup(self, target_url:str):
        '''
        Gets the Beautiful Soup object for a target page

        Parameters
        ----------
        target_url : str
            The page of interest.

        Returns
        -------
        soup : BeautifulSoup
            The object to search for HTML tags and types.
        '''
        print(f'DEBUG: fetching info from the url {target_url}')
        
        try:
            self.__mDriver.get(target_url)
            time.sleep(SLEEP_GET_SOUP_SEC)   # let the webpage load
            page_source = self.__mDriver.page_source
            
            # create a parser
            soup = BeautifulSoup(page_source, 'lxml')
        except Exception as e:
            print(f'ERROR: failed to retrieve info for url {target_url}')
            raise(e)
        
        return soup
    
    #-------------------------------------------------------------------------#
    
    #
    # Private
    #
    
    #-------------------------------------------------------------------------#
    
    def __get_table_df__(self, target_table:str, soup:BeautifulSoup):
        '''
        Get the data frame for the specified table id, if available. Otherwise
        returns an empty data frame.

        Parameters
        ----------
        target_table : str
            The id attribute for the table.
        soup : BeautifulSoup
            The HTML page source code to search.

        Returns
        -------
        df : pd.DataFrame
            The dataframe for the found table id.
        '''
        print(f'DEBUG: the target table is {target_table}')        
        tables = [table for table in soup.find_all('table')
                  if table.get('id') == target_table]
        
        if len(tables) != 1:
            print(f'ERROR: failed to find the table {target_table} in the passed in table element.')
            df = pd.DataFrame()
        else:
            df = get_simple_table_df(tables[0])
        return df
        
    #-------------------------------------------------------------------------#
        
    def __clean_df_names__(self, df:pd.DataFrame):
        '''
        Adds the team's city and name. Also cleans the 'Player' column if found

        Parameters
        ----------
        df : pd.DataFrame
            The data frame to modify.

        Returns
        -------
        df : pd.DataFrame
            The modified data frame.
        '''
        # Add columns for Team City and Team Name
        df['Team City'] = df['Tm'].map(lambda x: TEAM_ABR_TO_NAME[x][0])
        df['Team Name'] = df['Tm'].map(lambda x: TEAM_ABR_TO_NAME[x][1])
        
        # Review player names to remove asterisk (*) and plus-signs (+)
        pattern = '.*(?<![*+])' # keep all characters that precede a '*' and/or '+' 
        
        if 'Player' in df.columns:
            df['Player'] = df['Player'].map(lambda x: re.match(pattern, x).group())
        
        return df
    
    #-------------------------------------------------------------------------#
    
    def __save_to_csv__(self, df:pd.DataFrame, csv_name:str, csv_path:str, save_index:bool=False):
        '''
        Saves the DataFrame as a csv to the specied path

        Parameters
        ----------
        df : pd.DataFrame
            The data to save.
        csv_name : str
            The csv file name to save to.
        csv_path : str
            The path to save the csv file.
        save_index : bool
            If the index name/values of the DataFrame shoulde be saved

        Returns
        -------
        None.
        '''        
        # verify the save paths exists, create other wise        
        if not os.path.exists(csv_path):
            print(f'INFO: creating path for {csv_path}')
            os.makedirs(csv_path, exist_ok=True)
        
        csvFile = os.path.join(csv_path, csv_name)
        
        print(f'INFO: saving dataframe to {csvFile}')
        df.to_csv(csvFile, index=save_index)
    
#-----------------------------------------------------------------------------#


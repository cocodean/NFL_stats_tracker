import pandas as pd
import numpy as np

import bs4
from bs4 import BeautifulSoup
from selenium import webdriver

#import requests
import re
import os
import json
import time

from nfl_stats_scraper_constants import TEAM_ABR_TO_NAME, MAIN_URL, TARGET_GAME_TABLES, SLEEP_GET_SOUP_SEC
from table_to_dataframe_methods import METHOD_CONVERSION_MAP, get_simple_table_df,\
MC_GET_DF_IDX
import table_to_dataframe_methods as ttdm

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
    
    def get_home_away_teams(self, teams_playing_raw:str):
        '''
        Finds the home and away team name

        Parameters
        ----------
        teams_playing_raw : str
            The raw string taken from the html webpage.

        Returns
        -------
        away_out : str
            The away teams abr as defined in the constants file.
        home_out : str
            The home teams abr as defined in the constants file.
        '''
        away_out = ''
        home_out = ''
        
        # Expect format is: away_city away_team at home_city home_team - date
        teams_and_date = teams_playing_raw.split('-')
        
        if len(teams_and_date) != 2:
            print(f'ERROR: unrecognized away at home - date format -> {teams_playing_raw}')
            return away_out, home_out
            
        teams_raw = teams_and_date[0]
        teams = teams_raw.split(' at ')
        
        if len(teams) != 2:
            print(f'ERROR: unrecognized away_team at home_team format -> {teams}')
            return away_out, home_out
        
        away_team = teams[0].strip()
        home_team = teams[1].strip()
        
        away_found = False
        home_found = False
        for team_abr, team_parts in TEAM_ABR_TO_NAME.items():
            check_team_name = f'{team_parts[0]} {team_parts[1]}'
            if not away_found:
                if check_team_name == away_team:
                    away_out = team_abr
                    away_found = True
            if not home_found:
                if check_team_name == home_team:
                    home_out = team_abr
                    home_found = True
        
        if not away_found:
            print(f'ERROR: Did not find a match for the away team, {away_team}')
        
        if not home_found:
            print(f'ERROR: Did not find a match for the home team, {home_team}')
        
        return away_out, home_out
    
    #-------------------------------------------------------------------------#
    
    def get_game_stats(self, game_url:str, save_csv:bool, save_path:str):
        '''
        Parses the tables from the provided game url into dataframes

        Parameters
        ----------
        game_url : str
            The link to the site where the stats are.
        save_csv : bool
            If the csv files for the game's tables should be saved.
        save_path : str
            The dir path to save the files to.

        Returns
        -------
        game_dfs : dict
            A dictionary of data frames.
            Ex. games_dfs[table_name] = table_df
        '''
        try:
            #
            # Go to the page for the game and get the relevant tables to parse
            #
            game_dfs = dict()
            
            soup = self.get_soup(game_url)
            
            #
            # Get the Away vs Home teams
            #
            teams_playing = soup.find('div', role='main').h1.text
            hometeam, awayteam = self.get_home_away_teams(teams_playing)
            tp_d = {'Home Team': hometeam, 'Away Team': awayteam,
                    'Game URL': game_url}
            
            # save teams that are playing
            if save_csv:
                teams_playing_file = os.path.join(save_path, 'teams_playing.json')
                with open(teams_playing_file, 'w+') as fh:
                    json.dump(tp_d, fh)
            
            #
            # Get the linescore table separately
            #
            linescore_tables = [t for t in soup.find_all('table') if 'linescore' in t.get('class')]
            if len(linescore_tables) == 0:
                print(f'WARNING: No linescore table found for {game_url}\n')
            else:
                if len(linescore_tables) > 1:
                    print(f'WARNING: More than one, {len(linescore_tables)}, \
                          linsecore tables found for {game_url}. \
                              Using first found.\n')
                print('INFO: saving linescore dataframe\n')
                df = ttdm.get_linescore_df(linescore_tables[0])
                csvName = 'linescore.csv'
                self.__save_to_csv__(df, csvName, save_path)
            
            #
            # Get and process the target tables
            #
            tables = {table.get('id'): table for table in soup.find_all('table')
                      if table.get('id') in TARGET_GAME_TABLES}
            print(f'DEBUG: {len(tables)} tables found for {game_url}')
            
            # process each of the tables
            for tableName, tableData in tables.items():
                df = METHOD_CONVERSION_MAP[tableName][MC_GET_DF_IDX](tableData)
                
                game_dfs[tableName] = df
                
                if save_csv:
                    print(f'DEBUG: Saving the {tableName} dataframe to {save_path}')
                    csvName = f'{tableName}.csv'
                    self.__save_to_csv__(df, csvName, save_path)
        except Exception as e:
            print(f'ERROR: problem with getting game tables for {game_url}\nReason -> {e}\n\n')
        
        return game_dfs
    
    #-------------------------------------------------------------------------#
    
    def get_week_game_stats(self, year:int, week_n:int, save_csv:bool=False,
                            save_path:str='.'):
        '''
        Gets the DataFrames for a week's worth of games for the specified year
        Parameters
        ----------
        year : int
            The year to search statistics for. Limited to [1980, Present_Year]
        week_n : int
            The game week to get statistics for. Limited to 17 before 2021 and
            18 for 2021 and after.
        save_csv : bool
            If the csv files for the game's tables should be saved.
        save_path : str
            The dir path to save the files to.

        Returns
        -------
        games_dfs : dictionary
            dictionary of dictionaries of pd.DataFrame
            Ex. games_dfs['game_0']['team_stats'] = pd.DataFrame
        '''
        print(f'INFO: getting game stats for week {week_n} of year {year}')
            
        target_url = f'{MAIN_URL}/years/{year}/week_{week_n}.htm'
        
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
        try:
            for idx, table in enumerate(tables):
                links = table.find_all('a')     # look for all the anchor tags
                for link in links:
                    if 'boxscore' in link.get('href'):
                        links_map[idx] = MAIN_URL + link.get('href')
                        print(f'DEBUG: {idx} -> Adding the following link {links_map[idx]}')
                        break
        except Exception as e:
            print(f'ERROR: Problem getting game links for week {week_n}, year {year}. Reason -> {e}\n\n')
        #
        # Go to the page for each game and get the relevant tables to parse
        #
        games_dfs = dict()
        for game_idx, game_url in links_map.items():
            game_key = f'game_{game_idx}'
            game_dir = os.path.join(save_path, str(year), f'week_{week_n}', 
                                    game_key)
            if not os.path.exists(game_dir):
                print(f'WARNING: creating game directory {game_dir}')
                os.makedirs(game_dir, exist_ok=True)
                
            game_dfs = self.get_game_stats(game_url, save_csv, game_dir)
            
            games_dfs[game_key] = game_dfs
            
            # wait to try a new web page
            time.sleep(5)
        
        
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
    
    def __save_to_csv__(self, df:pd.DataFrame, csv_name:str, csv_path:str, 
                        save_index:bool=False):
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


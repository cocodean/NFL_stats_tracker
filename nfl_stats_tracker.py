import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from bs4 import BeautifulSoup
from selenium import webdriver

import requests
import re
import os
import time

from scraper_constants import TEAM_ABR_TO_NAME

# Constants
MAIN_URL = r'https://www.pro-football-reference.com'

TARGET_GAME_TABLES = ('game_info', 'team_stats', 'player_offense', 
                      'player_defense', 'returns', 'kicking',
                      'passing_advanced', 'rushing_advanced',
                      'receiving_advanced', 'defense_advanced')

class NFL_Stats_Scraper:
    #
    # Public Constants
    #
    
    MAX_PLAYERS_TO_RETURN = 150
    TOP_LEVEL_SAVE_DIR = 'stats'
    
    #-------------------------------------------------------------------------#
    
    def __init__(self):
        '''
        Default constructor

        Returns
        -------
        None.

        '''
        print("DEBUG: new NFL_Stats object created")
        self.mIsDataLoaded = False                      # Data can be reloaded
        self.mData = None                               # Holds one set of data at a time, for now
        
        # setup selenium driver
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--incognito')
        options.add_argument('--headless')
        self.__mDriver = webdriver.Chrome(options=options)
        
    #-------------------------------------------------------------------------#
    
    def get_rushing_stats(self, year:int, 
                          maxPlayers:np.uint8=MAX_PLAYERS_TO_RETURN,
                          saveToCsv:bool=False):
        '''
        Retrieves the rushing statistics for the year selected

        Parameters
        ----------
        year : int
            DESCRIPTION.
        maxPlayers : np.uint8, optional
            DESCRIPTION. The default is MAX_PLAYERS_TO_RETURN.
        saveToCsv : bool, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        stats : TYPE
            DESCRIPTION.

        '''
        print(f'INFO: getting rushing stats for the year {year}')
        
        table_id = 'rushing'
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/rushing.htm'
        
        stats = self.__get_stats__(target_url, 1)
        
        stats['Year'] = year
        
        if saveToCsv:
            self.__save_to_csv__(stats, year,'rushing')
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def get_passing_stats(self, year:int, 
                          maxPlayers:np.uint8=MAX_PLAYERS_TO_RETURN, 
                          saveToCsv:bool=False):
        '''
        Retrieves the passing statistics for the year selected

        Parameters
        ----------
        year : int
            DESCRIPTION.
        maxPlayers : np.uint8, optional
            DESCRIPTION. The default is MAX_PLAYERS_TO_RETURN.
        saveToCsv : bool, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        stats : TYPE
            DESCRIPTION.

        '''
        print(f'INFO: getting passing stats for the year {year}')
        
        table_id = 'passing'
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/passing.htm'
        
        stats = self.__get_stats__(target_url, 0)
        
        stats['Year'] = year
        
        if saveToCsv:
            self.__save_to_csv__(stats, year, 'passing')
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def get_week_game_stats(self, year:int, weekNum:int):
        '''

        Parameters
        ----------
        year : int
            DESCRIPTION.
        weekNum : int
            DESCRIPTION.

        Returns
        -------
        games_tables : TYPE
            DESCRIPTION.

        '''
        print(f'INFO: getting game stats for week {weekNum} of year {year}')
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/week_{weekNum}.htm'
        
        soup = self.getSoup(target_url)
        
        # get all the tables for the teams box scores, they hold the hyperlink
        # to the games stats page
        tables = [table for table in soup.find_all('table') 
                  if 'teams' in table.get('class')]
        
        
        #
        # Get the link to every game's boxscore for the week
        #
        links_map = dict()
        
        for idx, table in enumerate(tables):
            links = table.find_all('a')
            for link in links:
                if 'boxscore' in link.get('href'):
                    links_map[idx] = MAIN_URL + link.get('href')
                    print(f'DEBUG: {idx} -> Adding the following link {links_map[idx]}')
                    break
        
        #
        # Go to the page for each game and get the relevant tables to parse
        #
        games_tables = dict()
        for k,v in links_map.items():
            self.__mDriver.get(v)
            page_source = self.__mDriver.page_source
            
            soup = BeautifulSoup(page_source, 'lxml')
            
            teams_playing = soup.find('div', role='main').h1.text
            
            tables = {table.get('id'): table for table in soup.find_all('table')
                      if table.get('id') in TARGET_GAME_TABLES}
            print(f'{k}: {len(tables)}')
            
            games_tables[teams_playing] = tables
        
        for k,v in games_tables:
            print('Processing the following game: {k}')
            
        
        return games_tables
    
    #-------------------------------------------------------------------------#
    
#
# "Private"
#

    def get_soup(self, target_url):
        print(f'DEBUG: fetching info from the url {target_url}')
        
        try:
            pages = requests.get(target_url)
        except Exception as e:
            print(f'ERROR: failed to retrieve info for url {target_url}')
            raise(e)
            
        # create a parser
        soup = BeautifulSoup(pages.text, 'lxml')
        
        return soup
    
    #-------------------------------------------------------------------------#
    
    def __get_stats__(self, target_url:str, target_table:np.uint8=0,
                     num_rows_to_return:np.uint8=MAX_PLAYERS_TO_RETURN):
        '''
        General method to get stats from the site pro football reference.

        Parameters
        ----------
        target_url : str
            DESCRIPTION.
        target_table : np.uint8, optional
            DESCRIPTION. The default is 0.
        num_rows_to_return : np.uint8, optional
            DESCRIPTION. The default is MAX_PLAYERS_TO_RETURN.

        Returns
        -------
        stats : TYPE
            DESCRIPTION.

        '''
        print(f'DEBUG: fetching info from the url {target_url}')
        print(f'DEBUG: the target table is {target_table}')
        
        try:
            pages = requests.get(target_url)
        except Exception as e:
            print(f'ERROR: failed to retrieve info for url {target_url}')
            raise(e)
            
        # create a parser
        soup = BeautifulSoup(pages.text, 'lxml')
        
        # HTML tables have the 'tr' tag for rows. So we grab one row and find all its header values; Breaks for multiple tables
        headers = [th.getText() for th in soup.findAll('tr')[target_table].findAll('th')]
        headers = headers[1:] # drop the 'Rk' rank column since it will not match with the table
        print(f'DEBUG: the headers found for the table are {headers}')
        
        # Get the table rows
        rows = soup.findAll('tr', class_= lambda table_rows: table_rows != 'thead')
        print(f'DEBUG: the number of data rows found is {len(rows)}')
        
        # Extract player information from the table row tags
        player_stats = [[td.getText() for td in rows[i].findAll('td')] # get the table data cell text from each table data cell
                        for i in range(len(rows))] # for each row
        player_stats = player_stats[2:] # drop the first two empty rows
        
        stats = pd.DataFrame(player_stats, columns=headers)
        
        if len(stats) > num_rows_to_return:
            stats = stats[:num_rows_to_return]
        
        #
        # Cleanup DataFrame
        #
        
        # Remove players that played for multiple teams, '2TM', probably low anyways
        stats = stats[stats['Tm'].map(lambda x: x in TEAM_ABR_TO_NAME.keys())]
        
        # Add columns for Team City and Team Name
        stats['Team City'] = stats['Tm'].map(lambda x: TEAM_ABR_TO_NAME[x][0])
        stats['Team Name'] = stats['Tm'].map(lambda x: TEAM_ABR_TO_NAME[x][1])
        
        # Review player names to remove asterisk (*) and plus-signs (+)
        pattern = '.*(?<![*+])' # keep all characters that precede a '*' and/or '+' 
        stats['Player'] = stats['Player'].map(lambda x: re.match(pattern, x).group())
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def __save_to_csv__(self, pDf:pd.DataFrame, pYear:np.uint16, pStatType:str):
        '''
        Saves the DataFrame as a csv to the specied path

        Parameters
        ----------
        pDf : pd.DataFrame
            DESCRIPTION.
        pYear : np.uint16
            DESCRIPTION.
        pStatType : str
            DESCRIPTION.

        Returns
        -------
        None.

        '''
        filename = f'nfl_{pStatType}_{pYear}.csv'
        
        savePath = os.path.join(self.TOP_LEVEL_SAVE_DIR, f'{pYear}', f'{pStatType}') 
        fileSave = os.path.join(savePath, filename)
        
        # verify the save paths exists, create other wise
        print(f'DEBUG: saving to path {savePath}')
        
        if not os.path.exists(self.TOP_LEVEL_SAVE_DIR):
            os.mkdir(self.TOP_LEVEL_SAVE_DIR)
        
        subDir1 = os.path.join(self.TOP_LEVEL_SAVE_DIR, f'{pYear}')
        if not os.path.exists(subDir1):
            os.mkdir(subDir1)
        
        if not os.path.exists((savePath)):
            os.mkdir(savePath)
        
        print(f'INFO: saving {pYear} {pStatType} stats to {fileSave}')
        pDf.to_csv(fileSave)
    
    #-------------------------------------------------------------------------#
    
#-----------------------------------------------------------------------------#

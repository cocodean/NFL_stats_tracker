import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests
import re
import os

# Constants
FOOTBALL_MAIN_URL_PART = r'https://www.pro-football-reference.com'

class NFL_Stats:
    #
    # Public Constants
    #
    
    MAX_PLAYERS_TO_RETURN = 150
    TOP_LEVEL_SAVE_DIR = 'stats'
    
    TEAM_ABR_TO_NAME = {'ARI': ('Arizona', 'Cardinals'),
                        'ATL': ('Atlanta', 'Falcons'),
                        'BAL': ('Baltimore', 'Ravens'),
                        'BUF': ('Buffalo', 'Bills'),
                        'CAR': ('Carolina', 'Panthers'),
                        'CHI': ('Chicago', 'Bears'),
                        'CIN': ('Cincinnati', 'Bengals'),
                        'CLE': ('Clevland', 'Browns'),
                        'DAL': ('Dallas', 'Cowboys'),
                        'DEN': ('Denver', 'Broncos'),
                        'DET': ('Detroit', 'Lions'),
                        'GNB': ('Green Bay', 'Packers'),
                        'HOU': ('Houston', 'Texans'),
                        'IND': ('Indianapolis', 'Colts'),
                        'JAX': ('Jacksonville', 'Jaguars'),
                        'KAN': ('Kansas City', 'Chiefs'),
                        'LAC': ('Los Angeles', 'Chargers'),
                        'LAR': ('Los Angeles', 'Rams'),
                        'LVR': ('Las Vegas', 'Raiders'),
                        'MIA': ('Miami', 'Dolphins'),
                        'MIN': ('Minnesota', 'Vikings'),
                        'NOR': ('New Orleans', 'Saints'),
                        'NWE': ('New England', 'Patriots'),
                        'NYG': ('New York', 'Giants'),
                        'NYJ': ('New York', 'Jets'),
                        'PHI': ('Philadelphia', 'Eagles'),
                        'PIT': ('Pittsburgh', 'Steelers'),
                        'SEA': ('Seattle', 'Seahawks'),
                        'SFO': ('San Francisco', '49ers'),
                        'TAM': ('Tampa Bay', 'Buccaneers'),
                        'TEN': ('Tennessee', 'Titans'),
                        'WAS': ('Washington', 'Commanders')}
    
    #-------------------------------------------------------------------------#
    
    '''
    @brief Default constructor
    '''
    def __init__(self):
        print("DEBUG: new NFL_Stats object created")
        self.mIsDataLoaded = False                      # Data can be reloaded
        self.mData = None                               # Holds one set of data at a time, for now
        
    #-------------------------------------------------------------------------#
    
    '''
    @brief Retrieves the rushing statistics for the year selected
    @param[in] year An integer year to search stats for
    @param[in] maxPlayers An integer for the max rows to return in data frame
    @param[in] saveToCsv A Boolean if saving to csv file
    @return pandas DataFrame
    '''
    def getRushingStats(self, year:int, maxPlayers:np.uint8=MAX_PLAYERS_TO_RETURN, saveToCsv:bool=False):
        print(f'INFO: getting rushing stats for the year {year}')
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/rushing.htm'
        
        stats = self.__getStats__(target_url, 1)
        
        stats['Year'] = year
        
        if saveToCsv:
            self.__saveToCsv__(stats, year,'rushing')
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    '''
    @brief Retrieves the passing statistics for the year selected
    @param[in] year An integer year to search stats for
    @param[in] maxPlayers An integer for the max rows to return in data frame
    @param[in] saveToCsv A Boolean if saving to csv file
    @return pandas DataFrame
    '''
    def getPassingStats(self, year:int, maxPlayers:np.uint8=MAX_PLAYERS_TO_RETURN, saveToCsv:bool=False):
        print(f'INFO: getting passing stats for the year {year}')
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/passing.htm'
        
        stats = self.__getStats__(target_url, 0)
        
        stats['Year'] = year
        
        if saveToCsv:
            self.__saveToCsv__(stats, year, 'passing')
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def correlateRushingStats(self):
        print('INFO: the "correlateRushingStats" still needs an implementation')
        # TODO: how to bring years together
    
    #-------------------------------------------------------------------------#
    
#
# "Private"
#
    
    '''
    @brief General method to get stats from the site pro football reference.
    @param[in] target_url A string to represent the site to search
    @param[in] target_table An int for which table found to use
    @param[in] num_rows_to_return An int for the number of rows to allow in
    the return DataFrame
    @return pandas DataFrame
    '''
    def __getStats__(self, target_url:str, target_table:np.uint8=0,
                     num_rows_to_return:np.uint8=MAX_PLAYERS_TO_RETURN):
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
        stats = stats[stats['Tm'].map(lambda x: x in self.TEAM_ABR_TO_NAME.keys())]
        
        # Add columns for Team City and Team Name
        stats['Team City'] = stats['Tm'].map(lambda x: self.TEAM_ABR_TO_NAME[x][0])
        stats['Team Name'] = stats['Tm'].map(lambda x: self.TEAM_ABR_TO_NAME[x][1])
        
        # Review player names to remove asterisk (*) and plus-signs (+)
        pattern = '.*(?<![*+])' # keep all characters that precede a '*' and/or '+' 
        stats['Player'] = stats['Player'].map(lambda x: re.match(pattern, x).group())
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    '''
    @brief Saves the DataFrame as a csv to the specied path
    @param[in] pDf Pandas DataFrame to save
    @param[in] pYear The year to save under
    @param[in] pStatType The stat type being saved
    '''
    def __saveToCsv__(self, pDf:pd.DataFrame, pYear:np.uint16, pStatType:str):
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
    
    
    
    #-------------------------------------------------------------------------#
    
#-----------------------------------------------------------------------------#

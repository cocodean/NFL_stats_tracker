import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests

# Constants
FOOTBALL_MAIN_URL_PART = r'https://www.pro-football-reference.com'
FOOTBALL_RUSHING_URL_LAST_PART = r'rushing.htm'

class NFL_Stats:
    #
    # Public Constants
    #
    
    MAX_PLAYERS_TO_RETURN = 150
    

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
    @param[in] year An integer value in the range [1975, 2021]
    @return A DataFrame with the 
    @returnError Returns None if year for page is invalid
    '''
    def getRushingStats(self, year:int, printToCsv:bool=False):
        print(f'DEBUG: getting rushing stats for the year {year}')
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/rushing.htm'
        
        stats = self.__getStats__(target_url, 1)
        
        # TODO: cleanup player names to remove asterisk (*) and plus-signs (+)
        
        stats['Year'] = year
        
        if printToCsv:
            stats.to_csv(f'nfl_rushing_{year}.csv')     # TODO: edit name and path
        
        if len(stats) > self.MAX_PLAYERS_TO_RETURN:
            stats = stats[:self.MAX_PLAYERS_TO_RETURN]
        
        return stats
    
    #-------------------------------------------------------------------------#
    
    def getPassingStats(self, year:int, printToCsv:bool=False):
        print(f'DEBUG: getting passing stats for the year {year}')
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/passing.htm'
        
        stats = self.__getStats__(target_url, 0)
        
        # TODO: cleanup player names to remove asterisk (*) and plus-signs (+)
        
        stats['Year'] = year
        
        if printToCsv:
            stats.to_csv(f'nfl_passing_{year}.csv')     # TODO: edit name and path
        
        if len(stats) > self.MAX_PLAYERS_TO_RETURN:
            stats = stats[:self.MAX_PLAYERS_TO_RETURN]
        
        return stats
    
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
    @return A pandas DataFrame
    '''
    def __getStats__(self, target_url:str, 
                     target_table:int=0,
                     num_rows_to_return:int=MAX_PLAYERS_TO_RETURN):
        print(f'DEBUG: fetching info from the url {target_url}')
        print(f'DEBUG: the targetTable is {target_table}')
        
        try:
            pages = requests.get(target_url)
        except Exception as e:
            print(f'ERROR: failed to retrieve info for url {target_url}\n\n{e}')
            return None
            
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
        
        return stats
    
#-----------------------------------------------------------------------------#

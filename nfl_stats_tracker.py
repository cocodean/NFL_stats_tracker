import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests

# Constants
FOOTBALL_MAIN_URL_PART = r'https://www.pro-football-reference.com'
FOOTBALL_RUSHING_URL_LAST_PART = r'rushing.htm'

class NFL_Stats:
    
    '''
    @brief Default constructor
    '''
    def __init__(self):
        print("DEBUG: new NFL_Stats object created")
        self.mIsDataLoaded = False                      # Data can be reloaded
        self.mData = None                               # Holds one set of data at a time, for now
        
#-----------------------------------------------------------------------------#
    
    '''
    @brief Retrieves the rushing statistics for the year selected
    @param[in] year An integer value in the range [1975, 2021]
    @return A DataFrame with the 
    @returnError Returns None if year for page is invalid
    '''
    def getRushingStats(self, year:int, printToCsv:bool=False):
        print(f'DEBUG: getting rushing stats for the year {year}')
        
        MAX_PLAYERS_TO_RETURN = 150
        
        target_url = f'https://www.pro-football-reference.com/years/{year}/rushing.htm'
        
        print(f'DEBUG: fetching info from the url {target_url}')
        
        try:
            pages = requests.get(target_url)
        except Exception as e:
            print(f'ERROR: failed to retrieve info for url {target_url}\n\n{e}')
            return None
            
        # create a parser
        soup = BeautifulSoup(pages.text, 'lxml')
        
        # HTML tables have the 'tr' tag for rows. So we grab one row and find all its header values; Breaks for multiple tables
        headers = [th.getText() for th in soup.findAll('tr')[1].findAll('th')]
        headers = headers[1:] # drop the 'Rk' rank column since it will not match with the table
        
        # Get the table rows
        rows = soup.findAll('tr', class_= lambda table_rows: table_rows != 'thead')
        
        # Extract player information from the table row tags
        player_stats = [[td.getText() for td in rows[i].findAll('td')] # get the table data cell text from each table data cell
                        for i in range(len(rows))] # for each row
        player_stats = player_stats[2:] # drop the first two empty rows
        
        stats = pd.DataFrame(player_stats, columns=headers)
        
        # TODO: cleanup player names to remove asterisk (*) and plus-signs (+)
        
        stats['Year'] = year
        
        if printToCsv:
            stats.to_csv(f'nfl_rushing_{year}.csv')     # TODO: edit name and path
        
        if len(stats) > MAX_PLAYERS_TO_RETURN:
            stats = stats[:MAX_PLAYERS_TO_RETURN]
        
        return stats

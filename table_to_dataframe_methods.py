# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 14:33:08 2023

@author: cocod
"""

import pandas as pd
import bs4

from nfl_stats_scraper_constants import TEAM_ABR_TO_NAME

#----------------------------------------------------------------------------#

def get_simple_table_df(table:bs4.element.Tag):
    try:
        df = pd.read_html(table.prettify('utf-8'))[0]
    except Exception as e:
        print(f'ERROR: could not read html to create simple table into dataframe. Reason: {e}')
        df = pd.DataFrame()
    
    return df

#----------------------------------------------------------------------------#

def get_game_info_df(table:bs4.element.Tag):
    df = get_simple_table_df(table)
    
    return df

#----------------------------------------------------------------------------#

def get_team_stats_df(table:bs4.element.Tag):
    df = pd.read_html(table.prettify('utf-8'))[0]
    df = df.transpose()
    
    col_names = list(df.iloc[0].values)
    col_names = dict(zip(range(len(df.columns)), col_names))
    
    df.rename(columns=col_names, inplace=True)
    df.drop(['Unnamed: 0'], inplace=True)
    
    return df

#----------------------------------------------------------------------------#

def get_combined_multi_col_df(table:bs4.element.Tag, tableName:str):
    df = pd.DataFrame()
    
    try:
        df = pd.read_html(table.prettify('utf-8'))[0]
    
        multi_col_names = {'Unnamed: 0_level_0': 'Player', 
                           'Unnamed: 1_level_0': 'Team'}
        
        df.rename(columns=multi_col_names, level=0, inplace=True)
        
        mask = [v in TEAM_ABR_TO_NAME.keys() for v in df[('Team', 'Tm')]]
        
        df = df[mask]
        
        df.reset_index(drop=True, inplace=True)
    except Exception as e:
        print(f'ERROR: Failed to get the multi-column DataFrame for {tableName}\n{e}')
    
    return df

#----------------------------------------------------------------------------#

def get_player_offense_df(table:bs4.element.Tag):
    df = get_combined_multi_col_df(table, 'player_offense')
    
    return df


#----------------------------------------------------------------------------#

def get_player_defense_df(table:bs4.element.Tag):
    df = get_combined_multi_col_df(table, 'player_defense')
    
    return df

#----------------------------------------------------------------------------#

def get_returns_df(table:bs4.element.Tag):
    df = get_combined_multi_col_df(table, 'returns')
    
    return df

#----------------------------------------------------------------------------#

def get_kicking_df(table:bs4.element.Tag):
    df = get_combined_multi_col_df(table, 'kicking')
    
    return df

#----------------------------------------------------------------------------#

def get_simple_combined_df(table:bs4.element.Tag, tableName:str):
    df = pd.DataFrame()
    
    try:
        df = pd.read_html(table.prettify('utf-8'))[0]
        
        mask = [v in TEAM_ABR_TO_NAME.keys() for v in df['Tm']]
            
        df = df[mask]
        
        df.reset_index(drop=True, inplace=True)
    except Exception as e:
        print(f'ERROR: {e}')
    
    return df

#----------------------------------------------------------------------------#

def get_passsing_advanced_df(table:bs4.element.Tag):
    df = get_simple_combined_df(table, 'passing_advanced')
    
    return df

#----------------------------------------------------------------------------#

def get_rushing_advanced_df(table:bs4.element.Tag):
    df = get_simple_combined_df(table, 'rushing_advanced')
    
    return df

#----------------------------------------------------------------------------#

def get_receiving_advanced_df(table:bs4.element.Tag):
    df = get_simple_combined_df(table, 'receiving_advanced')
    
    return df

#----------------------------------------------------------------------------#

def get_defense_advanced_df(table:bs4.element.Tag):
    df = get_simple_combined_df(table, 'defense_advanced')
    
    return df

#----------------------------------------------------------------------------#

#
# Map for table parsing methods
#
METHOD_CONVERSION_MAP = {'game_info': get_game_info_df, 
                         'team_stats': get_team_stats_df,
                         'player_offense': get_player_offense_df,
                         'player_defense': get_player_defense_df,
                         'returns': get_returns_df,
                         'kicking': get_kicking_df,
                         'passing_advanced': get_passsing_advanced_df,
                         'rushing_advanced': get_rushing_advanced_df,
                         'receiving_advanced': get_receiving_advanced_df,
                         'defense_advanced': get_defense_advanced_df}

#----------------------------------------------------------------------------#


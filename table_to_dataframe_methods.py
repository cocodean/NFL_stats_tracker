# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 14:33:08 2023

@author: cocod
"""

import pandas as pd
import bs4

from scraper_constants import TEAM_ABR_TO_NAME

#----------------------------------------------------------------------------#

def get_game_info_df(table:bs4.element.Tag):
    df = pd.read_html(table.prettify('utf-8'))[0]
    
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

def get_player_offense_df(table:bs4.element.Tag):
    df = pd.read_html(table.prettify('utf-8'))[0]
    
    multi_col_names = {'Unnamed: 0_level_0': 'Player', 
                       'Unnamed: 1_level_0': 'Team'}
    
    df.rename(columns=multi_col_names, level=0, inplace=True)
    
    teams = set(df[('Team', 'Tm')].values)
    teams = [v for v in teams if v in TEAM_ABR_TO_NAME.keys()]
    
    df0 = df[df[('Team', 'Tm')] == teams[0]]
    df1 = df[df[('Team', 'Tm')] == teams[1]]


#----------------------------------------------------------------------------#


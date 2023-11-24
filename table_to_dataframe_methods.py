# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 14:33:08 2023

@author: cocod
"""

import pandas as pd
import numpy as np
import bs4
import os

import nfl_stats_scraper_constants as nssc
# from nfl_stats_scraper_constants import TEAM_ABR_TO_NAME

#----------------------------------------------------------------------------#

# 
# Methods used for scraping the HTML tables
#

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
        
        mask = [v in nssc.TEAM_ABR_TO_NAME.keys() for v in df[('Team', 'Tm')]]
        
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
        
        mask = [v in nssc.TEAM_ABR_TO_NAME.keys() for v in df['Tm']]
            
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
# Methods for combining the scraped tables
#

def convert_game_info_to_main(csv_path:str):
    pass

#----------------------------------------------------------------------------#

def convert_team_stats_to_main(csv_path:str, main_path:str, 
                               week_num:int, year:int):
    
    # try:
    # verify file exists
    if not os.path.exists(csv_path):
        print(f'ERROR: no team stats found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    # verify column has been updated
    if 'Unnamed: 0' in df.columns:
        # convert the unnamed column
        df.rename(columns={'Unnamed: 0': 'Team'}, inplace=True)
    
    # verify the number of teams
    teams = df['Team'].values
    if len(teams) != 2:
        print(f'WARNING: found {len(teams)} teams for csv {csv_path}')
        return
    
    # Get the stats for the teams
    for team in teams:
        if team not in nssc.TEAM_ABR_TO_NAME.keys():
            print(f'ERROR: Team -> {team} is not recognized.')
            continue
        
        dtemp = dict()
        
        for col in df.columns:
            val_col = df[col].values[0]
            if 'Team' == col:
                # skip
                continue
            elif 'Rush-Yds-TDs' == col:
                vals = val_col.split('-')
                
                feat_name = 'rushing attemtps game'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'rushing yards game'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'rushing touchdowns game'
                dtemp[feat_name] = int(vals[2])
            elif 'Cmp-Att-Yd-TD-INT' == col:
                vals = val_col.split('-')
                
                feat_name = 'passing completions game'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'passing attempts game'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'passing yards game'
                dtemp[feat_name] = int(vals[2])
                
                feat_name = 'passing touchdowns game'
                dtemp[feat_name] = int(vals[3])
                
                feat_name = 'passing interceptions game'
                dtemp[feat_name] = int(vals[4])
            elif 'Sacked-Yards' == col:
                vals = val_col.split('-')
                
                feat_name = 'offense sacked game'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'offense sacked yards game'
                dtemp[feat_name] = int(vals[1])
            elif 'Fumbles-Lost' == col:
                vals = val_col.split('-')
                
                feat_name = 'team fumbles game'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'team fumbles lost game'
                dtemp[feat_name] = int(vals[1])
            elif 'Penalties-Yards' == col:
                vals = val_col.split('-')
                
                feat_name = 'penalties game'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'penalty yards game'
                dtemp[feat_name] = int(vals[1])
            elif 'Third Down Conv.' == col:
                vals = val_col.split('-')
                
                feat_name = 'third down conversion attempts game'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'third down conversion successes game'
                dtemp[feat_name] = int(vals[0])
            elif 'Fourth Down Conv.' == col:
                vals = val_col.split('-')
                
                feat_name = 'fourth down conversion attempts game'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'fourth down conversion successes game'
                dtemp[feat_name] = int(vals[0])
            elif 'Time of Possession' == col:
                vals = val_col.split(':')
                
                total_mins = int(vals[0])/1.0 + int(vals[1])/60.0
                feat_name = col + ' game'
                dtemp[feat_name] = total_mins
            else:
                # process normally
                feat_name = col + ' game'
                dtemp[feat_name] = val_col
                
        dtemp['Week'] = week_num
        dtemp['Year'] = year
        
        # Add to the main team csv
        team_csv = os.path.join(main_path, f'{team}_season_{year}.csv')
        if os.path.exists(team_csv):
            df_team = pd.read_csv(team_csv)
        else:
            print(f'INFO: creating csv for {team_csv}')
            df_team = pd.DataFrame()
        
        df_temp = pd.DataFrame([dtemp])
        if 'Week' in df_team.columns and 'Year' in df_team.columns:
            df_team = pd.DataFrame.merge(df_team, df_temp, on=['Week', 'Year'])
        else:
            df_team = pd.concat([df_team, df_temp], axis=0)
        
        # save the updated dataframe
        df_team.to_csv(team_csv, index=False)
        
    # except Exception as e:
    #     print(f"ERROR: convert team stats for {csv_path}. Reason --> {e}\n")

#----------------------------------------------------------------------------#

def convert_player_offense_to_main(csv_path:str, main_path:str, 
                                   week_num:int, year:int):
    
    # verify file exists
    if not os.path.exists(csv_path):
        print(f'ERROR: no player offense stats found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    # verify the number of teams
    teams = set(df['Team'])
    teams = [team for team in teams if team in nssc.TEAM_ABR_TO_NAME.keys()]
    if len(teams) != 2:
        print(f'ERROR: found {len(teams)} teams for file {csv_path}')
        return
    
    # Get the stats for the teams
    for team in teams:
        df_team = df[df['Team'] == team]
        
        # dictionary used to build up the dataframe to add 
        dtemp = dict()
        dtemp['Week'] = week_num
        dtemp['Year'] = year
        # process each feature
        for feat_key in nssc.PLAYER_OFFENSE_COLUMN_MAP.keys():
            method = nssc.PLAYER_OFFENSE_COLUMN_MAP[feat_key][nssc.PO_METHOD_IDX]
            feat_name = nssc.PLAYER_OFFENSE_COLUMN_MAP[feat_key][nssc.PO_NAME_IDX]
            
            if feat_key not in df_team.columns:
                print(f'ERROR: did not find the feature {feat_key} in player offense csv')
                continue
            val = method(df_team[feat_key].astype(np.float))
            dtemp[feat_name] = val
        
        # open the team data frame to add the data
        team_csv = os.path.join(main_path, f'{team}_season_{year}.csv')
        if os.path.exists(team_csv):
            df_main = pd.read_csv(team_csv)
        else:
            print(f'INFO: creating csv for {team_csv}')
            df_main = pd.DataFrame()
        
        df_temp = pd.DataFrame([dtemp])
        df_main = pd.DataFrame.merge(df_main, df_temp, on=['Week', 'Year'])
        
        # save the updated dataframe
        df_main.to_csv(team_csv, index=False)

#----------------------------------------------------------------------------#

def convert_player_defense_to_main(csv_path:str, main_path:str, 
                                   week_num:int, year:int):
    # verify file exists
    if not os.path.exists(csv_path):
        print(f'ERROR: no player defense stats found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    # verify the number of teams
    teams = set(df['Team'])
    teams = [team for team in teams if team in nssc.TEAM_ABR_TO_NAME.keys()]
    if len(teams) != 2:
        print(f'ERROR: found {len(teams)} teams for file {csv_path}')
        return
    
    # Get the stats for the teams
    for team in teams:
        df_team = df[df['Team'] == team]
        
        # dictionary used to build up the dataframe to add 
        dtemp = dict()
        dtemp['Week'] = week_num
        dtemp['Year'] = year
        # process each feature
        for feat_key in nssc.PLAYER_DEFENSE_COLUMN_MAP.keys():
            method = nssc.PLAYER_DEFENSE_COLUMN_MAP[feat_key][nssc.PD_METHOD_IDX]
            feat_name = nssc.PLAYER_DEFENSE_COLUMN_MAP[feat_key][nssc.PD_NAME_IDX]
            
            if feat_key not in df_team.columns:
                print(f'ERROR: did not find the feature {feat_key} in player offense csv')
                continue
            
            val = method(df_team[feat_key].astype(np.float))
            dtemp[feat_name] = val
        
        # open the team data frame to add the data
        team_csv = os.path.join(main_path, f'{team}_season_{year}.csv')
        if os.path.exists(team_csv):
            df_main = pd.read_csv(team_csv)
        else:
            print(f'INFO: creating csv for {team_csv}')
            df_main = pd.DataFrame()
        
        df_temp = pd.DataFrame([dtemp])
        df_main = pd.DataFrame.merge(df_main, df_temp, on=['Week', 'Year'])
        
        # save the updated dataframe
        df_main.to_csv(team_csv, index=False)

#----------------------------------------------------------------------------#

def convert_kicking_to_main(csv_path:str, main_path:str, 
                                   week_num:int, year:int):
    # verify file exists
    if not os.path.exists(csv_path):
        print(f'ERROR: no kicking stats found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    # verify the number of teams
    teams = set(df['Team'])
    teams = [team for team in teams if team in nssc.TEAM_ABR_TO_NAME.keys()]
    if len(teams) != 2:
        print(f'ERROR: found {len(teams)} teams for file {csv_path}')
        return
    
    # Get the stats for the teams
    for team in teams:
        df_team = df[df['Team'] == team]
        
        # dictionary used to build up the dataframe to add 
        dtemp = dict()
        dtemp['Week'] = week_num
        dtemp['Year'] = year
        # process each feature
        for feat_key in nssc.KICKING_COLUMN_MAP.keys():
            method = nssc.KICKING_COLUMN_MAP[feat_key][nssc.K__METHOD_IDX]
            feat_name = nssc.KICKING_COLUMN_MAP[feat_key][nssc.K__NAME_IDX]
            
            if feat_key not in df_team.columns:
                print(f'ERROR: did not find the feature {feat_key} in player offense csv')
                continue
            
            val = method(df_team[feat_key].astype(np.float))
            dtemp[feat_name] = val
        
        # open the team data frame to add the data
        team_csv = os.path.join(main_path, f'{team}_season_{year}.csv')
        if os.path.exists(team_csv):
            df_main = pd.read_csv(team_csv)
        else:
            print(f'INFO: creating csv for {team_csv}')
            df_main = pd.DataFrame()
        
        df_temp = pd.DataFrame([dtemp])
        df_main = pd.DataFrame.merge(df_main, df_temp, on=['Week', 'Year'])
        
        # save the updated dataframe
        df_main.to_csv(team_csv, index=False)

#----------------------------------------------------------------------------#

def convert_returns_to_main(csv_path:str, main_path:str, 
                                   week_num:int, year:int):
    # verify file exists
    if not os.path.exists(csv_path):
        print(f'ERROR: no return stats found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    # verify the number of teams
    teams = set(df['Team'])
    teams = [team for team in teams if team in nssc.TEAM_ABR_TO_NAME.keys()]
    if len(teams) != 2:
        print(f'ERROR: found {len(teams)} teams for file {csv_path}')
        return
    
    # Get the stats for the teams
    for team in teams:
        df_team = df[df['Team'] == team]
        
        # dictionary used to build up the dataframe to add 
        dtemp = dict()
        dtemp['Week'] = week_num
        dtemp['Year'] = year
        # process each feature
        for feat_key in nssc.RETURNS_COLUMN_MAP.keys():
            method = nssc.RETURNS_COLUMN_MAP[feat_key][nssc.KR_METHOD_IDX]
            feat_name = nssc.RETURNS_COLUMN_MAP[feat_key][nssc.KR_NAME_IDX]
            
            if feat_key not in df_team.columns:
                print(f'ERROR: did not find the feature {feat_key} in player offense csv')
                continue
            
            val = method(df_team[feat_key].astype(np.float))
            dtemp[feat_name] = val
        
        # open the team data frame to add the data
        team_csv = os.path.join(main_path, f'{team}_season_{year}.csv')
        if os.path.exists(team_csv):
            df_main = pd.read_csv(team_csv)
        else:
            print(f'INFO: creating csv for {team_csv}')
            df_main = pd.DataFrame()
        
        df_temp = pd.DataFrame([dtemp])
        df_main = pd.DataFrame.merge(df_main, df_temp, on=['Week', 'Year'])
        
        # save the updated dataframe
        df_main.to_csv(team_csv, index=False)

#----------------------------------------------------------------------------#

''' 
    Map of methods for the different table types parsed from a weekly game
    found on,
    https://www.pro-football-reference.com
'''
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

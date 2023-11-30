# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 14:33:08 2023

@author: cocod
"""

import pandas as pd
import numpy as np
import bs4
import os
import re

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

def get_linescore_df(table:bs4.element.Tag):
    df = get_simple_table_df(table)
    df.rename(columns={'Unnamed: 0': 'Logo', 'Unnamed: 1': 'Team Name Full'}, inplace=True)
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
    
    teams = list(df.index)
    df['Team'] = teams
    
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

def save_df_to_main_csv(save_dir:str, team:str, week_n:int, year:int, 
                         df_in:pd.DataFrame, col_names:list):
    try:
        # Add to the main team csv
        team_csv = os.path.join(save_dir, f'{team}_season_{year}.csv')
        if os.path.exists(team_csv):
            df_team = pd.read_csv(team_csv)
        else:
            print(f'INFO: creating csv for {team_csv}')
            df_team = pd.DataFrame()
        
        col_set = set(df_team.columns)
        
        if 'Week' in df_team.columns and 'Year' in df_team.columns:
            # week and year columns exists
            # check if we have the desired week and year
            bool1 = df_team['Year'] == year
            bool2 = df_team['Week'] == week_n
            mask = bool1 & bool2
            df_bool = df_team[mask]
            
            if len(df_bool) == 1: # target week and year exist
                # check if target columns exist
                if len(col_set.intersection(col_names)) == 0:
                    # columns dont exist yet so just merge normally
                    print('INFO: first time seeing the team stats columns')
                    df_team = df_team.merge(df_in, how='inner', on=['Week', 'Year'])
                else:
                    # columns exist so have to overwrite values
                    idx = df_bool.index.tolist()[0]
                    on_cols = ['Year', 'Week']
                    on_cols.extend(col_names)
                    tdf = df_bool[on_cols]
                    tdf = tdf.merge(df_in, how='right', on=on_cols)
                    for col in col_names:
                        df_team.loc[idx, col] = tdf.iloc[0][col]
                
            elif len(df_bool) == 0:
                print('INFO: team stats concat')
                df_team = pd.concat([df_team, df_in], axis=0)
            else:
                print('ERROR: check how we are applying the mask')
                
        else:
            # no week or year columns, most likely a new dataframe
            df_team = pd.concat([df_team, df_in], axis=0)
        
        # save the updated dataframe
        df_team.to_csv(team_csv, index=False)
    except Exception as e:
        print(f'ERROR: Failed to save the data frame to the main csv. Reason -> {e}\n')
        print(f'ERROR: params,\nteam: {team},\nweek: {week_n},\nyear: {year},\
              \ntarget columns: {col_names},\ndf columns: {df_in.columns},\nsave dir: {save_dir}\n')

#----------------------------------------------------------------------------#

def convert_linescore_to_main(csv_path:str, save_dir:str,
                              week_n:int, year:int):
    if not os.path.exists(csv_path):
        print(f'ERROR: no linescore csv found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    teams = list(df['Team Name Full'])
    if len(teams) != 2:
        print(f'ERROR: Different number of teams found, {len(teams)}')
    
    for team in teams:
        # find with team it belongs to
        team_abr = ''
        for k, v in nssc.TEAM_ABR_TO_NAME.items():
            if f'{v[0]} {v[1]}' == team:
                team_abr = k
                break
        
        if '' == team_abr:
            print(f'ERROR: Failed to find a match for {team}')
            continue
        sdf = df[df['Team Name Full'] == team]
        dtemp = dict()
        dtemp['Year'] = year
        dtemp['Week'] = week_n
        
        LINESCORE_TABLE_COLUMNS = [
            'LS First Quarter Pts Scored',
            'LS Second Quarter Pts Scored',
            'LS Third Quarter Pts Scored',
            'LS Fourth Quarter Pts Scored',
            'LS Final Score',
            'LS First Quarter Pts Gave Up',
            'LS Second Quarter Pts Gave Up',
            'LS Third Quarter Pts Gave Up',
            'LS Fourth Quarter Pts Gave Up',
            'LS Total Points Gave Up',
            'LS Won Game'
            ]
        
        dtemp['LS First Quarter Pts Scored'] = sdf['1'].values[0]
        dtemp['LS Second Quarter Pts Scored'] = sdf['2'].values[0]
        dtemp['LS Third Quarter Pts Scored'] = sdf['3'].values[0]
        dtemp['LS Fourth Quarter Pts Scored'] = sdf['4'].values[0]
        team_final_score = sdf['Final'].values[0]
        dtemp['LS Final Score'] = team_final_score
        
        odf = df[df['Team Name Full'] != team]
        dtemp['LS First Quarter Pts Gave Up'] = odf['1'].values[0]
        dtemp['LS Second Quarter Pts Gave Up'] = odf['2'].values[0]
        dtemp['LS Third Quarter Pts Gave Up'] = odf['2'].values[0]
        dtemp['LS Fourth Quarter Pts Gave Up'] = odf['3'].values[0]
        other_team_final_score = odf['Final'].values[0]
        dtemp['LS Total Points Gave Up'] = other_team_final_score
        
        dtemp['LS Won Game'] = 1 if team_final_score > other_team_final_score else 0
        
        df_temp = pd.DataFrame([dtemp])
        
        save_df_to_main_csv(save_dir, team_abr, week_n, year, df_temp, LINESCORE_TABLE_COLUMNS)

#----------------------------------------------------------------------------#

def convert_game_info_to_main(csv_path:str, save_dir:str, 
                              week_n:int, year:int):
    try:
        df = pd.read_csv(csv_path)
        
        # values used to add to dataframe
        favored_team = ''
        odds = 0
        over_under = -1
        
        #
        # Get the Vegas Line
        #
        vl_ser = df[df['Game Info'] == 'Vegas Line']['Game Info.1']
        vl_vals = vl_ser.values
        
        vegas_line_raw = vl_vals[0]
        vals = vegas_line_raw.split('-')
        
        if len(vals) != 2:
            print(f'WARNING: Did not find a recognized format, {vegas_line_raw}, for vegas line for the file {csv_path}.\
                  Assuming +100 odds for both teams')
            odds = 100
        else:            
            # get the odds and  favored team name
            teamFullName = vals[0].strip()
            odds = float(vals[1])
            
            # check if we recognize the team
            teamFound = False
            for k, v in nssc.TEAM_ABR_TO_NAME.items():
                if f'{v[0]} {v[1]}' == teamFullName:
                    favored_team = k
                    teamFound = True
                    break
            
            if not teamFound:
                print(f'ERROR: Failed to find a match for the favored team {teamFullName}')
        
        #
        # Get the over/under
        #
        ou_ser = df[df['Game Info'] == 'Over/Under']['Game Info.1']
        ou_vals = ou_ser.values
        
        over_under_raw = ou_vals[0]
        pattern = '(\d+\.\d+)' # 1 or more digits followed by '.' followed by 1 or more digits
        
        val = re.search(pattern, over_under_raw)
        if None == val:
            print(f'ERROR: Failed to parse out the over under from {over_under_raw} for {csv_path}')
            over_under = -1
        else:
            val = float(val[0])
            over_under = val
            
        #
        # Add to dataframe
        #
        
        # determine what teams are playing
        team_stats_csv = os.path.join(os.path.dirname(csv_path), 'team_stats.csv')
        df_ts = pd.read_csv(team_stats_csv)
        
        # verify column has been updated
        if 'Unnamed: 0' in df_ts.columns:
            # convert the unnamed column
            df_ts.rename(columns={'Unnamed: 0': 'Team'}, inplace=True)
    
        teams = [t for t in df_ts['Team'] if t in nssc.TEAM_ABR_TO_NAME.keys()]
    
        # verify the number of teams
        teams = df_ts['Team'].values
        if len(teams) != 2:
            print(f'ERROR: found {len(teams)} teams for csv {team_stats_csv}')
            return
        
        GAME_INFO_COLUMNS = ['GI Vegas Line', 'GI Over/Under']
        for team in teams:
            dtemp = dict()
            dtemp['Year'] = year
            dtemp['Week'] = week_n
            
            dtemp['GI Vegas Line'] = odds * -1 if team == favored_team else odds
            dtemp['GI Over/Under'] = over_under
            
            df_temp = pd.DataFrame([dtemp])
            
            save_df_to_main_csv(save_dir, team, week_n, year, df_temp, GAME_INFO_COLUMNS)
        
    except Exception as e:
        print(f'ERROR: convert_game_info_to_main, Reason -> {e}')

#----------------------------------------------------------------------------#

def convert_team_stats_to_main(csv_path:str, save_dir:str, 
                               week_n:int, year:int):
    
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
                
                feat_name = 'TS Rushing Attemtps'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'TS Rushing Yards'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'TS Rushing TDs'
                dtemp[feat_name] = int(vals[2])
            elif 'Cmp-Att-Yd-TD-INT' == col:
                vals = val_col.split('-')
                
                feat_name = 'TS Passing Completions'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'TS Passing Attempts'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'TS Passing Yards'
                dtemp[feat_name] = int(vals[2])
                
                feat_name = 'TS Passing TDs'
                dtemp[feat_name] = int(vals[3])
                
                feat_name = 'TS Passing INTs'
                dtemp[feat_name] = int(vals[4])
            elif 'Sacked-Yards' == col:
                vals = val_col.split('-')
                
                feat_name = 'TS Offensive Sacks'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'TS Offensive Sacks Yards'
                dtemp[feat_name] = int(vals[1])
            elif 'Fumbles-Lost' == col:
                vals = val_col.split('-')
                
                feat_name = 'TS Team Fumbles'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'TS Team Fumbles Lost'
                dtemp[feat_name] = int(vals[1])
            elif 'Penalties-Yards' == col:
                vals = val_col.split('-')
                
                feat_name = 'TS Penalties'
                dtemp[feat_name] = int(vals[0])
                
                feat_name = 'TS Penalty Yards'
                dtemp[feat_name] = int(vals[1])
            elif 'Third Down Conv.' == col:
                vals = val_col.split('-')
                
                feat_name = 'TS 3rd Down Conversion Attempts'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'TS 3rd Down Conversion Successes'
                dtemp[feat_name] = int(vals[0])
            elif 'Fourth Down Conv.' == col:
                vals = val_col.split('-')
                
                feat_name = 'TS 4th Down Conversion Attempts'
                dtemp[feat_name] = int(vals[1])
                
                feat_name = 'TS 4th Down Conversion Successes'
                dtemp[feat_name] = int(vals[0])
            elif 'Time of Possession' == col:
                vals = val_col.split(':')
                
                total_mins = int(vals[0])/1.0 + int(vals[1])/60.0
                feat_name = 'TS ' + col
                dtemp[feat_name] = total_mins
            else:
                # process normally
                feat_name = 'TS ' + col
                dtemp[feat_name] = val_col
                
        dtemp['Week'] = week_n
        dtemp['Year'] = year
        
        df_temp = pd.DataFrame([dtemp])
        
        save_df_to_main_csv(save_dir, team, week_n, year, df_temp, nssc.TEAM_STATS_COLUMNS)
        
    # except Exception as e:
    #     print(f"ERROR: convert team stats for {csv_path}. Reason --> {e}\n")

#----------------------------------------------------------------------------#

def convert_table_to_main(table_name: str, csv_path:str, save_dir:str, 
                          week_n:int, year:int):
    # verify file exists
    if not os.path.exists(csv_path):
        print(f'ERROR: no {table_name} stats found for {csv_path}')
        return
    
    # get csv data
    df = pd.read_csv(csv_path)
    
    # verify the number of teams
    teams = set(df['Team'])
    teams = [team for team in teams if team in nssc.TEAM_ABR_TO_NAME.keys()]
    if len(teams) != 2:
        print(f'WARNING: only found {len(teams)} teams for file {csv_path}')
    
    if table_name not in nssc.MASTER_TABLE_COLUMN_MAP.keys():
        print(f'ERROR: {table_name} not found for column map')
        return
    
    table_column_map = nssc.MASTER_TABLE_COLUMN_MAP[table_name]
    if None == table_column_map:
        print(f'WARNING: no valid map found for {table_name}')
        return
    
    # Get the stats for the teams
    for team in teams:
        df_team = df[df['Team'] == team]
        
        # dictionary used to build up the dataframe to add 
        dtemp = dict()
        
        # process each feature
        for feat_key in table_column_map.keys():
            method = table_column_map[feat_key][nssc.MASTER_METHOD_IDX]
            feat_name = table_column_map[feat_key][nssc.MASTER_NAME_IDX]
            
            if feat_key not in df_team.columns:
                print(f'ERROR: did not find the feature {feat_key} in {table_name} csv')
                continue
            val = method(df_team[feat_key].astype(np.float))
            dtemp[feat_name] = val
        
        col_names = list(dtemp.keys())
        
        dtemp['Week'] = week_n
        dtemp['Year'] = year
        
        # get dataframe to add of the converted data
        df_temp = pd.DataFrame([dtemp])
        
        save_df_to_main_csv(save_dir, team, week_n, year, df_temp, col_names)

#----------------------------------------------------------------------------#

def convert_player_offense_to_main(csv_path:str, save_dir:str, 
                                   week_n:int, year:int):
    convert_table_to_main('player offense', csv_path, save_dir, week_n, year)

#----------------------------------------------------------------------------#

def convert_player_defense_to_main(csv_path:str, save_dir:str, 
                                   week_n:int, year:int):
    convert_table_to_main('player defense', csv_path, save_dir, week_n, year)

#----------------------------------------------------------------------------#

def convert_kicking_to_main(csv_path:str, save_dir:str, 
                            week_n:int, year:int):
    convert_table_to_main('kicking', csv_path, save_dir, week_n, year)

#----------------------------------------------------------------------------#

def convert_returns_to_main(csv_path:str, save_dir:str, 
                            week_n:int, year:int):
    convert_table_to_main('returns', csv_path, save_dir, week_n, year)

#----------------------------------------------------------------------------#

''' 
    Map of methods for the different table types parsed from a weekly game
    found on,
    https://www.pro-football-reference.com
'''
MC_GET_DF_IDX = 0
MC_CONV_IDX = 1
METHOD_CONVERSION_MAP = {
    'linescore': (get_linescore_df, convert_linescore_to_main),
    'game_info': (get_game_info_df, convert_game_info_to_main),
    'team_stats': (get_team_stats_df, convert_team_stats_to_main),
    'player_offense': (get_player_offense_df, convert_player_offense_to_main),
    'player_defense': (get_player_defense_df, convert_player_defense_to_main),
    'returns': (get_returns_df, convert_returns_to_main),
    'kicking': (get_kicking_df, convert_kicking_to_main),
    'passing_advanced': (get_passsing_advanced_df, None),
    'rushing_advanced': (get_rushing_advanced_df, None),
    'receiving_advanced': (get_receiving_advanced_df, None),
    'defense_advanced': (get_defense_advanced_df, None)
                         }

#----------------------------------------------------------------------------#

# 
# Methods used to combine the individual tables scraped using NFL_Stats_Scraper
#
def combine_game_stats(game_dir:str, save_path:str, week_n:int, year:int):
    if not os.path.exists(game_dir):
        print(f'ERROR: the game dir provided is not valid, {game_dir}')
        return
    
    # combine all the data csvs 
    for table_name in METHOD_CONVERSION_MAP.keys():
        # get the convert method to use
        converter = METHOD_CONVERSION_MAP[table_name][1]
        
        if None == converter:
            print(f'WARNING: There is no conversion method for {table_name}')
            continue
        
        csv_file = os.path.join(game_dir, f'{table_name}.csv')
        if not os.path.exists(csv_file):
            print(f'ERROR: The csv file for {table_name} does not exist, {csv_file}')
            continue
        
        # process into the team' main season data frame
        converter(csv_file, save_path, week_n, year)
        
    

#-------------------------------------------------------------------------#

def combine_week_stats(week_dir:str, save_path:str, week_n:int, year:int):
    # assume 16-game week. Let the error detection handle less games
    TOTAL_GAMES = 16
    
    contents = next(os.walk(week_dir))
    DIR_NAMES_IDX = 1
    total_dirs = len(contents[DIR_NAMES_IDX])
    print(f'total folders found is {total_dirs}')
    N_GAMES = total_dirs if total_dirs <= TOTAL_GAMES else TOTAL_GAMES
    
    for game_n in range(N_GAMES):
        # check for directory of possible game
        game_dir = os.path.join(week_dir, f'game_{game_n}')
        if not os.path.exists(game_dir):
            print(f'WARNING: failed to find directory for game {game_n}, {game_dir}')
            continue
        
        combine_game_stats(game_dir, save_path, week_n, year)

#-------------------------------------------------------------------------#

def combine_season_stats(year_dir:str, save_path:str, year:int,):
    # assume 18-week season. Let the error detection handle less weeks
    TOTAL_WEEKS = 18
    
    # Cycle through all the weeks of the season
    for week_n in range(1, TOTAL_WEEKS + 1):
        week_dir = os.path.join(year_dir, f'week_{week_n}')
        if not os.path.exists(week_dir):
            print(f'ERROR: The week folder {week_dir} does not exist')
            continue
        
        combine_week_stats(week_dir, save_path, week_n, year)

#-------------------------------------------------------------------------#

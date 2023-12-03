# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 21:41:44 2023

@author: cocod
"""

import pandas as pd
import os
import json

import nfl_stats_scraper_constants as nssc

#-----------------------------------------------------------------------------#

def combine_and_save_derived_df_to_main(df_in:pd.DataFrame, team_csv:str):
    # we are assuming there is no overlap for columns in the incoming derived  
    # dataframe and the team dataframe. we will stack the two dataframes 
    # vertically, so they should be the same length.
    df_comb = pd.DataFrame()
    
    # Get the team's main dataframe
    if os.path.exists(team_csv):
        df_team = pd.read_csv(team_csv)
    else:
        print(f'ERROR: The team csv, {team_csv}, could not be found. Nothing to add to.\n')
        return df_comb
    
    if len(df_team) != len(df_in):
        print(f'ERROR: the derived df has a different length than the team df\n\
              team->{len(df_team)},\tinput->{len(df_in)}\n')
        return df_comb
    
    df_comb = pd.concat([df_team, df_in], axis=1)
    df_comb.to_csv(team_csv, index=False)
    
    return df_comb
#-----------------------------------------------------------------------------#

# TODO: make the below more robust or clarify method name
def save_df_to_main(save_dir:str, team:str, week_n:int, year:int, 
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

#-----------------------------------------------------------------------------#

def create_season_dataset(year_dir:int, year:int, save_path:str, save_csv:bool=True):
    if not os.path.exists(year_dir):
        print(f'ERROR: the season directory does not exist, {year_dir}\n')
        return
    
    teams_season_dir = os.path.join(year_dir, 'team_season_stats')
    
    if not os.path.exists(year_dir):
        print(f'ERROR: the teams season directory does not exist, {teams_season_dir}\n')
        return
    
    lod = list()
    for week_n in range(1, nssc.N_WEEKS_SEASON + 1):
        week_dir = os.path.join(year_dir, f'week_{week_n}')
        if not os.path.exists(week_dir):
            print(f'WARNING: the week directory does not exist, {week_dir}\n')
            continue
        
        for game_n in range(nssc.N_GAMES_WEEK):
            dtemp = dict()
            dtemp['Year'] = year
            dtemp['Week'] = week_n
            dtemp['Game Index'] = game_n
            
            game_dir = os.path.join(week_dir, f'game_{game_n}')
            if not os.path.exists(game_dir):
                print(f'WARNING: the game directory does not exist, {game_dir}\n')
                continue
            
            # get home and away teams
            teams_json = os.path.join(game_dir, 'teams_playing.json')
            with open(teams_json, 'r') as fh:
                teams_data = json.load(fh)
            
            # if either are empty, we will assume WAS since that one is giving
            # trouble with same city but different team names
            home_team = teams_data['Home Team']
            away_team = teams_data['Away Team']
            
            dtemp['Home Team'] = home_team
            dtemp['Away Team'] = away_team
            
            # get home and away teams' dataframe
            home_csv = os.path.join(teams_season_dir, f'{home_team}_season_{year}.csv')
            away_csv = os.path.join(teams_season_dir, f'{away_team}_season_{year}.csv')
            
            df_home = pd.read_csv(home_csv)
            df_away = pd.read_csv(away_csv)
            
            sdf_home = df_home[df_home['Week'] == week_n]
            if len(sdf_home) != 1:
                print(f'ERROR: {year}-{week_n}-{game_n} length of home team is {len(sdf_home)}\n')
                
            sdf_away = df_away[df_away['Week'] == week_n]
            if len(sdf_away) != 1:
                print(f'ERROR: {year}-{week_n}-{game_n} length of away team is {len(sdf_away)}\n')
            
            # add the away and home team data, both should have the same column names
            print(sdf_away.columns)
            for col in sdf_away.columns:
                if col in ['Year', 'Week']:
                    continue
                elif col[:3] in nssc.BASE_FEATURE_PREFIXES:
                    # dont keep any base features from the game tables
                    continue
                
                away_feat_name = f'Away {col}'
                dtemp[away_feat_name] = sdf_away[col].values[0]
                
                home_feat_name = f'Home {col}'
                dtemp[home_feat_name] = sdf_home[col].values[0]
                
            # determine if the home team won
            home_score = sdf_home['LS Final Score'].values[0]
            away_score = sdf_away['LS Final Score'].values[0]
            dtemp['Winning Team'] = 1 if home_score > away_score else 0
            
            # add to the list of dictionaries to be used for dataframe constuctor
            lod.append(dtemp)
            
    df_season = pd.DataFrame(lod)
    
    # save the season dataset
    if save_csv:
        season_csv = os.path.join(save_path, f'season_{year}_dataset.csv')
        df_season.to_csv(season_csv, index=False)
    
    return df_season

#-----------------------------------------------------------------------------#

def create_mult_season_datasets(start_year:int, end_year:int, stats_dir:str, save_path:str):
    if not os.path.exists(stats_dir):
        print(f'ERROR: the season directory does not exist, {stats_dir}\n')
        return
    
    for year in range(start_year, end_year + 1):
        year_dir = os.path.join(stats_dir, str(year))
        
        create_season_dataset(year_dir, year, save_path)
    
    print(f'INFO: finished creating season datasets for years [{start_year}, {end_year}]\n')

#-----------------------------------------------------------------------------#

def create_train_val_test_sets(stats_dir:str, train_years:list, 
                               validation_years:list, test_years:list,
                               save_path:str):
    if not os.path.exists(stats_dir):
        print(f'ERROR: Nothing to create, the stats directory does not exist, {stats_dir}\n')
        return
    
    df_train = pd.DataFrame()
    for year in train_years:
        season_csv = os.path.join(stats_dir, f'season_{year}_dataset.csv')
        df_season = pd.read_csv(season_csv)
        sdf = df_season[df_season['Week'] > 1]
        
        df_train = pd.concat([df_train, sdf])
        df_train.reset_index(inplace=True, drop=True)
    
    df_validate = pd.DataFrame()
    for year in validation_years:
        season_csv = os.path.join(stats_dir, f'season_{year}_dataset.csv')
        df_season = pd.read_csv(season_csv)
        sdf = df_season[df_season['Week'] > 1]
        
        df_validate = pd.concat([df_validate, sdf])
        df_validate.reset_index(inplace=True, drop=True)
    
    df_test = pd.DataFrame()
    for year in test_years:
        season_csv = os.path.join(stats_dir, f'season_{year}_dataset.csv')
        df_season = pd.read_csv(season_csv)
        sdf = df_season[df_season['Week'] > 1]
        
        df_test = pd.concat([df_test, sdf])
        df_test.reset_index(inplace=True, drop=True)
    
    # save all datasets
    train_csv = os.path.join(save_path, 'train_dataset.csv')
    df_train.to_csv(train_csv, index=False)
    
    validate_csv = os.path.join(save_path, 'validate_dataset.csv')
    df_validate.to_csv(validate_csv, index=False)
    
    test_csv = os.path.join(save_path, 'test_dataset.csv')
    df_test.to_csv(test_csv, index=False)

#-----------------------------------------------------------------------------#


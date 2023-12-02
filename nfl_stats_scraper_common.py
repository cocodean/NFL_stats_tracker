# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 21:41:44 2023

@author: cocod
"""

import pandas as pd
import os

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
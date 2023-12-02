# -*- coding: utf-8 -*-
"""
@brief: Methods used to derive features based on the features already taken 
from the HTMl tables scraped and data observed

@author: cocod
"""
import pandas as pd
import numpy as np
import os

import nfl_stats_scraper_constants as nssc
import nfl_stats_scraper_common as nsscom

#-----------------------------------------------------------------------------#

def sum_n_rows(df_in:pd.DataFrame, col_name:str, n:int):
    N = len(df_in) if len(df_in) < n else n
    
    # calculate the sum
    if col_name in df_in.columns:
        # verify the number of nan terms is zero
        sdf = df_in[col_name][-N:]
        
        if len(sdf) != len(sdf.dropna()):
            print(f'WARNING: There are nan values present for sum column {col_name}. Setting those values to zero.')
            sdf = sdf.fillna(0)
        s = np.sum(sdf)
    else:
        print(f'ERROR: did not find {col_name} in the supplied data frame.\n')
        s = 0
    
    return s

#-----------------------------------------------------------------------------#

def avg_n_rows(df_in:pd.DataFrame, col_name:str, n:int):
    N = len(df_in) if len(df_in) < n else n
    
    if col_name in df_in.columns:
        # verify the number of nan terms is zero
        sdf = df_in[col_name][-N:]
        
        if len(sdf) != len(sdf.dropna()):
            print(f'WARNING: There are nan values present for avg column {col_name}. Setting those values to zero.')
            sdf = sdf.fillna(0)
        avg = np.sum(sdf) / float(N)
    else:
        print(f'ERROR: did not find {col_name} in the supplied data frame.\n')
        avg = 0
        
    return avg

#-----------------------------------------------------------------------------#

def derive_season_k_features(team_csv:str, year:int, base_cols:list, table_type:str,
                             calc_season_sum:bool=True):
    if not os.path.exists(team_csv):
        print(f'ERROR: Failed to find the team dataframe from {team_csv}\n')
        return
    df_team = pd.read_csv(team_csv)
    
    lod = list() # list of dicts to build the derived features dataframe
    for week_n in range(1, nssc.N_WEEKS_SEASON + 1):
        if not(week_n in df_team['Week'].values):
            print(f'ERROR: did not find week {week_n} in the {table_type} dataframe.')
            continue
        
        dtemp = dict()
        dtemp['Year'] = year
        dtemp['Week'] = week_n
        
        for base_feat in base_cols:
            # remove table tag prefix
            base_feat_name = base_feat[3:] # LS
            der_feat_name_season = f'{base_feat_name} season'
            der_feat_name_k = 'Avg. {} last {} games'
            
            # handle the week 1 special case
            if 1 == week_n:
                if calc_season_sum:
                    # initialize derived season stats
                    dtemp[der_feat_name_season] = 0
                
                # initialize last k game stats
                for k in nssc.K_VALS:
                    dtemp[der_feat_name_k.format(base_feat_name, k)] = 0
                
                continue
            
            # sub dataframe with the specific weeks set
            sdf = df_team[df_team['Week'] <= (week_n - 1)]
            
            if calc_season_sum:
                # calculate derived season stats
                val_sum = np.sum(sdf[base_feat])
                dtemp[der_feat_name_season] = val_sum
            
            # calculate last k game stats
            for k in nssc.K_VALS:
                der_avg_k = avg_n_rows(sdf, base_feat, k)
                dtemp[der_feat_name_k.format(base_feat_name, k)] = der_avg_k
        
        lod.append(dtemp)
    
    df_derive = pd.DataFrame(lod)
    
    return df_derive

#-----------------------------------------------------------------------------#

def derive_linescore_features(team_csv:str, year:int):
    df_derive = derive_season_k_features(team_csv, year, 
                                         list(nssc.LINESCORE_TABLE_COLUMNS), 
                                         'linescore')
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], 
                                                              axis=1), 
                                               team_csv)
    
    return df_derive

#-----------------------------------------------------------------------------#

def derive_team_stats_features(team_csv:str, year:int):
    df_derive = derive_season_k_features(team_csv, year, 
                                         list(nssc.TEAM_STATS_COLUMNS), 
                                         'team stats')
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    # calculate the following statistics
    df_team = pd.read_csv(team_csv)
    calc_cols = list()
    
    # 3rd down conversion rate and get derived k
    feat_name = 'TS 3rd Down Conversion Rate'
    calc_cols.append(feat_name)
    df_3rdAtt = df_team['TS 3rd Down Conversion Attempts']
    df_3rdSuc = df_team['TS 3rd Down Conversion Successes']
    df_team[feat_name] = df_3rdSuc / df_3rdAtt
    df_team[feat_name].fillna(0, inplace=True) # not likely to happen here
    
    # 4th down conversion rate and get derived k
    feat_name = 'TS 4th Down Conversion Rate'
    calc_cols.append(feat_name)
    df_4thAtt = df_team['TS 4th Down Conversion Attempts']
    df_4thSuc = df_team['TS 4th Down Conversion Successes']
    df_team[feat_name] = df_4thSuc / df_4thAtt
    df_team[feat_name].fillna(0, inplace=True) # more likely to happen here
    
    # rushing yards per attempt and get derived k
    feat_name = 'TS Rushing Yards Per Attempt'
    calc_cols.append(feat_name)
    df_rushAtt = df_team['TS Rushing Attemtps']
    df_rushYds = df_team['TS Rushing Yards']
    df_team[feat_name] = df_rushYds / df_rushAtt
    
    # passing yards per completion and get derived k
    feat_name = 'TS Passing Yards Per Completion'
    calc_cols.append(feat_name)
    df_passCmp = df_team['TS Passing Completions']
    df_passYds = df_team['TS Passing Yards']
    df_team[feat_name] = df_passYds / df_passCmp
    
    # passing completion rate and get derived k
    feat_name = 'TS Passing Completion Rate'
    calc_cols.append(feat_name)
    df_passAtt = df_team['TS Passing Attempts']
    df_team[feat_name] = df_passCmp / df_passAtt
    
    # write to original csv file
    df_team.to_csv(team_csv, index=False)
    
    df_derive = derive_season_k_features(team_csv, year, 
                                         calc_cols, 
                                         'team stats calculated rates',
                                          calc_season_sum=False)
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    return df_derive

#-----------------------------------------------------------------------------#

def derive_player_defense_features(team_csv:str, year:int):
    defense_cols = [val[nssc.PD_NAME_IDX] for val in 
                    nssc.PLAYER_DEFENSE_COLUMN_MAP.values()]
    
    df_derive = derive_season_k_features(team_csv, year, 
                                         defense_cols, 
                                         'player defense')
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    return df_derive

#-----------------------------------------------------------------------------#

def derive_player_offense_features(team_csv:str, year:int):
    offense_cols = [val[nssc.PO_NAME_IDX] for val in 
                    nssc.PLAYER_OFFENSE_COLUMN_MAP.values()]
    
    df_derive = derive_season_k_features(team_csv, year, 
                                         offense_cols, 
                                         'player offense')
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    return df_derive

#-----------------------------------------------------------------------------#

def derive_kicking_features(team_csv:str, year:int):
    kicking_cols = [val[nssc.K__NAME_IDX] for val in 
                    nssc.KICKING_COLUMN_MAP.values()]
    
    df_derive = derive_season_k_features(team_csv, year, 
                                         kicking_cols, 
                                         'kicking')
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    # calculate the following statistics
    df_team = pd.read_csv(team_csv)
    calc_cols = list()
    
    # extra point success rate and get derived k
    feat_name = 'K_ Extra Point Success Rate'
    calc_cols.append(feat_name)
    df_xpAtt = df_team['K_ Extra Point Attempts']
    df_xpSuc = df_team['K_ Extra Points Made']
    df_team[feat_name] = df_xpSuc / df_xpAtt
    df_team[feat_name].fillna(0, inplace=True) # might happen here
    
    # field goal success rate and get derived k
    feat_name = 'K_ Field Goal Success Rate'
    calc_cols.append(feat_name)
    df_fgAtt = df_team['K_ Field Goal Attempts']
    df_fgSuc = df_team['K_ Field Goals Made']
    df_team[feat_name] = df_fgSuc / df_fgAtt
    df_team[feat_name].fillna(0, inplace=True) # might happen here
    
    # write to original csv file
    df_team.to_csv(team_csv, index=False)
    
    df_derive = derive_season_k_features(team_csv, year, 
                                         calc_cols, 
                                         'kicking calculated rates',
                                          calc_season_sum=False)
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    return df_derive

#-----------------------------------------------------------------------------#

def derive_returns_features(team_csv:str, year:int):
    returns_cols = [val[nssc.KR_NAME_IDX] for val in 
                    nssc.RETURNS_COLUMN_MAP.values()]
    
    df_derive = derive_season_k_features(team_csv, year, 
                                         returns_cols, 
                                         'returns')
    
    # save the derived dataframe to the team's main dataframe
    nsscom.combine_and_save_derived_df_to_main(df_derive.drop(['Year', 'Week'], axis=1), 
                                               team_csv)
    
    return df_derive

#-----------------------------------------------------------------------------#

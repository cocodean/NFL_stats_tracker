# -*- coding: utf-8 -*-
"""
Created on Sun Nov 26 11:58:07 2023

@author: cocod
"""

import pandas as pd
import table_to_dataframe_methods as abc
import nfl_stats_scraper_constants as nssc





year_dir = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\2011'
save_path = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\2011\team_season_stats'
year = 2011

abc.combine_season_stats(year_dir, save_path, year)







# # check all the file got created for each game folder
# import os
# stats_dir = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats'

# contents = next(os.walk(stats_dir))
# DIR_IDX = 1
# FILE_IDX = 2
# for year_dir_name in contents[DIR_IDX]:
#     if 'archive' == year_dir_name:
#         continue
    
#     year_dir = os.path.join(stats_dir, year_dir_name)
#     year_contents = next(os.walk(year_dir))
    
#     for week_dir_name in year_contents[DIR_IDX]:
#         week_dir = os.path.join(year_dir, week_dir_name)
#         week_contents = next(os.walk(week_dir))
        
#         for game_dir_name in week_contents[DIR_IDX]:
#             game_dir = os.path.join(week_dir, game_dir_name)
#             game_contents = next(os.walk(game_dir))
            
#             num_files = len(game_contents[FILE_IDX])
#             if num_files <= 1:
#                 print(f'ERROR: {year_dir_name}-{week_dir_name}-{game_dir_name} only found 1 or lest files\n')
#             elif num_files <= 7 and num_files > 1:
#                 print(f'WARNING: {year_dir_name}-{week_dir_name}-{game_dir_name} <-> {num_files}')



















# team1_csv = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\sandbox\2002\teams_season_stats\SFO_season_2002.csv"
# team2_csv = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\sandbox\2002\teams_season_stats\NYG_season_2002.csv"
'''
df1 = pd.read_csv(team1_csv)
df2 = pd.read_csv(team2_csv)

PO_COLS = [v[0] for v in nssc.PLAYER_OFFENSE_COLUMN_MAP.values()]
PD_COLS = [v[0] for v in nssc.PLAYER_DEFENSE_COLUMN_MAP.values()]
K__COLS = [v[0] for v in nssc.KICKING_COLUMN_MAP.values()]
KR_COLS = [v[0] for v in nssc.RETURNS_COLUMN_MAP.values()]


# combine data from 2 to 1
sub1 = df2[PO_COLS].copy()
sub1['Week'] = 2
sub1['Year'] = 2002

df = pd.concat([df1, sub1], axis=0)
df.reset_index(inplace=True, drop=True)

#------

sub2 = df2[PD_COLS].copy()
sub2['Week'] = 2
sub2['Year'] = 2002

tgt_cols = ['Year', 'Week']
tgt_cols.extend(PD_COLS)

mask = (df['Week'] == 2) & (df['Year'] == 2002)
idx = df.index[mask].tolist()[0]
tdf = df[mask][tgt_cols].copy()
tdf = tdf.merge(sub2, on=tgt_cols, how='right')
for col in tdf.columns:
    df.loc[idx, col] = tdf.iloc[0][col]

#------

sub3 = df2[K__COLS].copy()
sub3['Week'] = 2
sub3['Year'] = 2002

#------

sub4 = df2[KR_COLS].copy()
sub4['Week'] = 2
sub4['Year'] = 2002
'''



# -*- coding: utf-8 -*-
"""
Created on Fri Nov 10 09:34:55 2023

@author: cocod
"""

import os
import time

import nfl_stats_scraper
import table_to_dataframe_methods as tdm
import derive_features_methods as dfm
import nfl_stats_scraper_constants as nssc

'''
17 weeks per season until 2021, then 18 week season
'''

def scrape_nfl_season_games_for_years(year_start:int, year_end:int, save_path:str):
    scraper = nfl_stats_scraper.NFL_Stats_Scraper()
    
    # Get the 18-week seasons, and let the exception be caught for the 
    # 17-week seasons
    SEASON_LENGTH_WEEKS = 18
    for year in  range(year_start, year_end):
        for week in range(1, SEASON_LENGTH_WEEKS):
            try:
                dfs = scraper.get_week_game_stats(year, week, 
                                                  save_csv=True, 
                                                  save_path=save_path)
                time.sleep(5)
            except Exception as e:
                print(f'ERROR: failed to get the info for year:{year}, week:{week}, reason: {e}\n')

#-----------------------------------------------------------------------------#

def combine_nfl_season_games(start_year:int, end_year:int, save_path:str):
    print('Combining year data for {start_year} - {end_year}')
    for year in range(start_year, end_year):
        team_season_save_path = os.path.join(save_path, str(year), 'team_season_stats')
        if not os.path.exists(team_season_save_path):
            print(f'INFO: creating team season save path, {team_season_save_path}')    
            os.makedirs(team_season_save_path)
        
        year_dir = os.path.join(save_path, str(year))
        tdm.combine_season_stats(year_dir, team_season_save_path, year)

#-----------------------------------------------------------------------------#

def add_derive_stats_for_team(team_csv:str, year:int, team:str):
    if not os.path.exists(team_csv):
        print(f'ERROR: nothing to add to, the path does not exist for {team_csv}')
        return
    
    df_ls = dfm.derive_linescore_features(team_csv, year)
    
    df_ts = dfm.derive_team_stats_features(team_csv, year)
    
    df_po = dfm.derive_player_offense_features(team_csv, year)
    
    df_pd = dfm.derive_player_defense_features(team_csv, year)
    
    df_k = dfm.derive_kicking_features(team_csv, year)
    
    df_kr = dfm.derive_returns_features(team_csv, year)
    
    print(f'INFO: completed adding derived stats for {team}-{year}')

#-----------------------------------------------------------------------------#

def add_derive_stats_for_all_teams(teams_year_stats_dir:str, year:int):
    if not os.path.exists(teams_year_stats_dir):
        print(f'ERROR: stats directory does not exist, {stats_dir}')
        return
    
    # cycle through all the teams
    for team in nssc.TEAM_ABR_TO_NAME.keys():
        team_csv = os.path.join(teams_year_stats_dir, f'{team}_season_{year}.csv')
        if not os.path.exists(team_csv):
            print(f'ERROR: did not find the season stats for {team}-{year}, {team_csv}\n')
            continue
        
        add_derive_stats_for_team(team_csv, year, team)

#-----------------------------------------------------------------------------#

def add_derive_stats_for_teams_years(stats_dir:str, year_start:int, year_end:int):
    if not os.path.exists(stats_dir):
        print(f'ERROR: the stats directory does not exist, {stats_dir}\n')
        return
    
    # cycle through years
    for year in range(year_start, year_end + 1):
        year_dir = os.path.join(stats_dir, str(year))
        if not os.path.exists(year_dir):
            print(f'ERROR: the {year} year directory does not exist, {year_dir}\n')
            continue
        
        print(f'INFO: adding data for year-{year}')
        
        seas_stats_dir = os.path.join(year_dir, 'team_season_stats')
        if not os.path.exists(seas_stats_dir):
            print(f'ERROR: the season stats directory for teams does not exist, {seas_stats_dir}\n')
            continue
        
        add_derive_stats_for_all_teams(seas_stats_dir, year)

#-----------------------------------------------------------------------------#

if __name__ == '__main__':
    # print('Collecting data')
    # start_year = 2002
    # end_year = 2022
    # save_path = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats'
    # scrape_nfl_season_games_for_years(start_year, end_year, save_path)
    
    # print('Combining year data')
    # start_year = 2002
    # start_year = 2007
    # start_year = 2009
    # end_year = 2022
    # save_path = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats'
    # combine_nfl_season_games(start_year, end_year, save_path)
    
    print('Adding the derived data')
    # team_csv = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\sandbox\2002\team_season_stats\ATL_season_2002.csv"
    # year = 2002
    # team = 'ATL'
    # add_derive_stats_for_team(team_csv, year, team)
    
    # team_stats_dir = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\sandbox\2002\team_season_stats'
    # add_derive_stats_for_teams_year(team_stats_dir, year)
    
    stats_dir = r'C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats'
    start_year = 2002
    end_year = 2021
    add_derive_stats_for_teams_years(stats_dir, start_year, end_year)
    
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 15:08:57 2023

@author: cocod
"""
import numpy as np

#
# Common Constants
#
MAIN_URL = r'https://www.pro-football-reference.com'

TEAM_ABR_TO_NAME = {'ARI': ('Arizona', 'Cardinals'),
                        'ATL': ('Atlanta', 'Falcons'),
                        'BAL': ('Baltimore', 'Ravens'),
                        'BUF': ('Buffalo', 'Bills'),
                        'CAR': ('Carolina', 'Panthers'),
                        'CHI': ('Chicago', 'Bears'),
                        'CIN': ('Cincinnati', 'Bengals'),
                        'CLE': ('Cleveland', 'Browns'),
                        'DAL': ('Dallas', 'Cowboys'),
                        'DEN': ('Denver', 'Broncos'),
                        'DET': ('Detroit', 'Lions'),
                        'GNB': ('Green Bay', 'Packers'),
                        'HOU': ('Houston', 'Texans'),
                        'IND': ('Indianapolis', 'Colts'),
                        'JAX': ('Jacksonville', 'Jaguars'),
                        'KAN': ('Kansas City', 'Chiefs'),
                        'SDG': ('San Diego', 'Chargers'),
                        'LAC': ('Los Angeles', 'Chargers'),
                        'STL': ('St. Louis', 'Rams'),
                        'LAR': ('Los Angeles', 'Rams'),
                        'OAK': ('Oakland', 'Raiders'),
                        'LVR': ('Las Vegas', 'Raiders'),
                        'MIA': ('Miami', 'Dolphins'),
                        'MIN': ('Minnesota', 'Vikings'),
                        'NOR': ('New Orleans', 'Saints'),
                        'NWE': ('New England', 'Patriots'),
                        'NYG': ('New York', 'Giants'),
                        'NYJ': ('New York', 'Jets'),
                        'PHI': ('Philadelphia', 'Eagles'),
                        'PIT': ('Pittsburgh', 'Steelers'),
                        'SEA': ('Seattle', 'Seahawks'),
                        'SFO': ('San Francisco', '49ers'),
                        'TAM': ('Tampa Bay', 'Buccaneers'),
                        'TEN': ('Tennessee', 'Titans'),
                        'WAS': ('Washington', 'Redskins')
                        # 'WAS': ('Washington', 'Commanders')
                        }

TEAM_CITY_IDX = 0
TEAM_NAME_IDX = 1

# The number of seconds to sleep between webpage source code requests
SLEEP_GET_SOUP_SEC = 5

#-----------------------------------------------------------------------------#

N_WEEKS_SEASON = 18 # 17-weeks up to 2020, then 18-weeks 2021 and on
K_VALS = (1, 2, 3, 5)

#-----------------------------------------------------------------------------#

#
# HTML Table Constants
#
TARGET_GAME_TABLES = ('game_info', 
                      'team_stats', 
                      'player_offense', 
                      'player_defense', 
                      'returns', 
                      'kicking',
                      'passing_advanced', 
                      'rushing_advanced',
                      'receiving_advanced', 
                      'defense_advanced')

#
# Constants for converting HTML tables to feature names
#
TABLE_COLUMN_ABR_TO_NAME = {
    'Cmp': 'Completions',
    'Att': 'Attempts',
    'Yds': 'Yards',
    'TD': 'Touchdowns',
    'TDs': 'Touchdowns',
    'Conv.': 'Conversion',
    'Rush': 'Rushes',
    'Sk': 'Sacks',
    'Lng': 'Long',
    }

simple_sum = lambda colX: 0 if len(colX.dropna()) == 0 else np.nansum(colX)
simple_max = lambda xCol: 0 if len(xCol.dropna()) == 0 else np.nanmax(xCol)
calc_avg_rate = lambda xCol: 0 if len(xCol.dropna()) == 0 else np.nansum(xCol) / np.sum([1.0 for x in xCol if isinstance(x, float)])

LINESCORE_TABLE_COLUMNS = (
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
            )

GAME_INFO_COLUMNS = ('GI Vegas Line',
                     'GI Over/Under')

TEAM_STATS_COLUMNS = ('TS First Downs',	
                      'TS Rushing Attemtps',
                      'TS Rushing Yards',
                      'TS Rushing TDs',
                      'TS Passing Completions',
                      'TS Passing Attempts',
                      'TS Passing Yards',
                      'TS Passing TDs',
                      'TS Passing INTs',
                      'TS Offensive Sacks',
                      'TS Offensive Sacks Yards',
                      'TS Net Pass Yards',
                      'TS Total Yards',
                      'TS Team Fumbles',
                      'TS Team Fumbles Lost',
                      'TS Turnovers',
                      'TS Penalties',
                      'TS Penalty Yards',
                      'TS 3rd Down Conversion Attempts',
                      'TS 3rd Down Conversion Successes',
                      'TS 4th Down Conversion Attempts',
                      'TS 4th Down Conversion Successes',
                      'TS Time of Possession')

PO_NAME_IDX = 0 # The name of the converted column in the output csv
PO_METHOD_IDX = 1 # The method used to convert the column into a single value
PLAYER_OFFENSE_COLUMN_MAP = {
        'Passing': ('PO Passing Completions', simple_sum),
        'Passing.1': ('PO Passing Attempts', simple_sum),
        'Passing.2': ('PO Passing Yards', simple_sum),
        'Passing.3': ('PO Passing TDs', simple_sum),
        'Passing.4': ('PO Passing Interceptions', simple_sum),
        'Passing.5': ('PO Passing Sacks', simple_sum),
        'Passing.6': ('PO Passing Sack Yards Loss', simple_sum),
        'Passing.7': ('PO Passing Long', simple_max),
        'Passing.8': ('PO Passing Rate', calc_avg_rate),
        'Rushing': ('PO Rushing Attempts', simple_sum),
        'Rushing.1': ('PO Rushing Yards', simple_sum),
        'Rushing.2': ('PO Rushing TDs', simple_sum),
        'Rushing.3': ('PO Rushing Long', simple_max),
        'Receiving': ('PO Receiving Targets',simple_sum),
        'Receiving.1': ('PO Receiving Receptions', simple_sum),
        'Receiving.2': ('PO Receiving Yards', simple_sum),
        'Receiving.3': ('PO Receiving TDs', simple_sum),
        'Receiving.4': ('PO Receiving Long', simple_max),
        'Fumbles': ('PO Offense Fumbles', simple_sum),
        'Fumbles.1': ('PO Offense Fumbles Yards Loss', simple_sum)
                    }


PD_NAME_IDX = 0 # The name of the converted column in the output csv
PD_METHOD_IDX = 1 # The method used to convert the column into a single value
PLAYER_DEFENSE_COLUMN_MAP = {
    'Def Interceptions': ('PD Defense Interceptions', simple_sum),
    'Def Interceptions.1': ('PD Defense Interceptions Yards', simple_sum),
    'Def Interceptions.2': ('PD Defense Interceptions TDs', simple_sum),
    'Def Interceptions.3': ('PD Defense Interceptions Long Yards', simple_max),
    'Def Interceptions.4': ('PD Defense Pass Deflections', simple_sum),
    'Unnamed: 7_level_0': ('PD Defense Sacks', simple_sum),
    'Tackles': ('PD Defense Tackles Combined', simple_sum),
    'Tackles.1': ('PD Defense Solo Tackles', simple_sum),
    'Tackles.2': ('PD Defense Tackle Assists', simple_sum),
    'Tackles.3': ('PD Defense Tackles for Loss', simple_sum),
    'Tackles.4': ('PD Defense QB Hits', simple_sum),
    'Fumbles': ('PD Defense Fumbles Recovered', simple_sum),
    'Fumbles.1': ('PD Defense Fumbles Yards', simple_sum),
    'Fumbles.2': ('PD Defense Fumbles TDs', simple_sum),
    'Fumbles.3': ('PD Defense Forced Fumbles', simple_sum)
    }


K__NAME_IDX = 0 # The name of the converted column in the output csv
K__METHOD_IDX = 1 # The method used to convert the column into a single value
KICKING_COLUMN_MAP = {
    'Scoring': ('K_ Extra Points Made', simple_sum),
    'Scoring.1': ('K_ Extra Point Attempts', simple_sum),
    'Scoring.2': ('K_ Field Goals Made', simple_sum),
    'Scoring.3': ('K_ Field Goal Attempts', simple_sum),
    'Punting': ('K_ Punts Total', simple_sum),
    'Punting.1': ('K_ Punt Yards Total', simple_sum),
    'Punting.2': ('K_ Punting Yards/Punt', calc_avg_rate),
    'Punting.3': ('K_ Punting Long Yards', simple_max)
    }


KR_NAME_IDX = 0 # The name of the converted column in the output csv
KR_METHOD_IDX = 1 # The method used to convert the column into a single value
RETURNS_COLUMN_MAP = {
    'Kick Returns': ('KR Kick Return Attempts', simple_sum),
    'Kick Returns.1': ('KR Kick Return Yards', simple_sum),
    'Kick Returns.2': ('KR Kick Return Yards Avg', calc_avg_rate),
    'Kick Returns.3': ('KR Kick Return TDs', simple_sum),
    'Kick Returns.4': ('KR Kick Return Long Yards', simple_max),
    'Punt Returns': ('KR Punt Return Attempts', simple_sum),
    'Punt Returns.1': ('KR Punt Return Yards', simple_sum),
    'Punt Returns.2': ('KR Punt Return Yards Avg', calc_avg_rate),
    'Punt Returns.3': ('KR Punt Return TDs', simple_sum),
    'Punt Returns.4': ('KR Punt Return Long Yards', simple_max)
    }

MASTER_NAME_IDX = 0
MASTER_METHOD_IDX = 1
MASTER_TABLE_COLUMN_MAP = {
    'game info': None,
    'team stats': None,
    'player offense': PLAYER_OFFENSE_COLUMN_MAP,
    'player defense': PLAYER_DEFENSE_COLUMN_MAP,
    'kicking': KICKING_COLUMN_MAP,
    'returns': RETURNS_COLUMN_MAP
    }

#-----------------------------------------------------------------------------#

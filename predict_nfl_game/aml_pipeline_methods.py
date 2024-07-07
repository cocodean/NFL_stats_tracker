import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import sys
NFL_STATS_DIR = r'..\\'
sys.path.append(NFL_STATS_DIR)
import nfl_stats_scraper_constants as nssc

# Used for data normalization
from sklearn.preprocessing import StandardScaler

from sklearn import svm
from sklearn import metrics

from sklearn.decomposition import PCA

#
# Configurables
#
# TEST_CSV = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\season_datasets\raw_combined_datasets\test_dataset.csv"
# TRAIN_CSV = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\season_datasets\raw_combined_datasets\train_dataset.csv"

TRAIN_CSV = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\season_datasets\raw_combined_datasets\train_test_only\train_dataset.csv"
TEST_2017_CSV = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\season_datasets\raw_combined_datasets\train_test_only\test_dataset_2017.csv"
TEST_2018_CSV = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\season_datasets\raw_combined_datasets\train_test_only\test_dataset_2018.csv"
TEST_2019_CSV = r"C:\Users\cocod\Work\Repos\NFL_stats_tracker\stats\season_datasets\raw_combined_datasets\train_test_only\test_dataset_2019.csv"

#-----------------------------------------------------------------------------#

def pull_train_test_datasets():
    if not os.path.exists(TRAIN_CSV):
        print(f'ERROR: the train dataset does not exist, {TRAIN_CSV}\n')
    
    if not os.path.exists(TEST_2017_CSV):
        print(f'ERROR: the test dataset does not exist, {TEST_2017_CSV}\n')
    
    if not os.path.exists(TEST_2018_CSV):
        print(f'ERROR: the test dataset does not exist, {TEST_2018_CSV}\n')
    
    if not os.path.exists(TEST_2019_CSV):
        print(f'ERROR: the test dataset does not exist, {TEST_2019_CSV}\n')
    
    df_train = pd.read_csv(TRAIN_CSV)
    df_test2017 = pd.read_csv(TEST_2017_CSV)
    df_test2018 = pd.read_csv(TEST_2018_CSV)
    df_test2019 = pd.read_csv(TEST_2019_CSV)
    
    return df_train, (df_test2017, df_test2018, df_test2019)

#-----------------------------------------------------------------------------#

def remove_special_cols(df_in:pd.DataFrame):
    cols_set = df_in.columns.to_list().copy()
    print(f'INFO: col set start size, {len(cols_set)}')
    
    # remove the kicking
    kicking_cols = [val[0][3:] for val in nssc.KICKING_COLUMN_MAP.values()]
    tgt_cols = list()
    for col in cols_set:
        found_match = False
        for c in kicking_cols:
            if c in col:
                found_match = True
                break
        if not found_match:
            tgt_cols.append(col)
    print(f'INFO: col set after removed kicking, {len(tgt_cols)}')
    
    # remove returns
    tgt2_cols = list()
    returns_cols = [val[0][3:] for val in nssc.RETURNS_COLUMN_MAP.values()]
    for col in tgt_cols:
        found_match = False
        for c in returns_cols:
            if c in col:
                found_match = True
                break
        if not found_match:
            tgt2_cols.append(col)
    print(f'INFO: col set after removed returns, {len(tgt2_cols)}')
    
    # remove last 5 games
    s = 'last 5 games'
    tgt3_cols = [col for col in tgt2_cols if not(s in col)]
    print(f'INFO: col set after removed last 5 games, {len(tgt3_cols)}')
    
    # remove Second Quarter
    s2nd = 'Second Quarter Pts'
    tgt4_cols = [col for col in tgt3_cols if not(s2nd in col)]
    print(f'INFO: col set after removed {s2nd}, {len(tgt4_cols)}')
    
    # remove Third Quarter
    s3rd = 'Third Quarter'
    tgt5_cols = [col for col in tgt4_cols if not(s3rd in col)]
    print(f'INFO: col set after removed {s3rd}, {len(tgt5_cols)}')
    
    # remove player offense set
    cols_po = [val[0][3:] for val in nssc.PLAYER_OFFENSE_COLUMN_MAP.values()]
    cols_po

    po_cols_keep = ['Passing Long', 'Passing Rate', 'Rushing Long',  'Offense Fumbles', 'Offense Fumbles Yards Loss']
    
    for col in po_cols_keep:
        cols_po.remove(col)
    
    # remove redundant player offense columns/features
    tgt6_cols = list()

    for col in tgt5_cols:
        found_match = False
        for c in cols_po:
            if c in col:
                found_match = True
                break
        if not found_match:
            tgt6_cols.append(col)
    print(f'INFO: col set after removed player offense, {len(tgt6_cols)}')
    
    df_out = df_in[tgt6_cols]
    
    print(f'INFO: Length of new df is {df_out.shape}')
    
    return df_out

#-----------------------------------------------------------------------------#

def drop_unwanted_columns(df_in:pd.DataFrame, cols_remove:list):
    cols_all = df_in.columns.to_list().copy()
    tgt_cols = [col for col in cols_all if not(col in cols_remove)]
    df_out = df_in[tgt_cols]
    
    return df_out

#-----------------------------------------------------------------------------#

def replace_any_nan_values(df_in:pd.DataFrame):
    df_out = df_in.copy()
    for col in df_in.columns:
        lorig = len(df_in[col])
        ldrop = len(df_in[col].dropna())
        if lorig != ldrop:
            print(f'Found a difference for train {col}, {lorig} vs {ldrop}')
            df_out[col] = df_in[col].fillna(0)
    
    return df_out

#-----------------------------------------------------------------------------#

def get_features_labels_from_df(df_in:pd.DataFrame):
    train_cols = df_in.columns.to_list().copy()
    
    takeout_cols = ['Year', 'Week', 'Home Team', 'Away Team']
    tgt_cols = [col for col in train_cols if not(col in takeout_cols)]
    
    x_cols = [col for col in tgt_cols if 'Winning Team' != col]
    
    y_col = ['Winning Team'] # 1 for home team and 0 for away team
    
    x_features = df_in[x_cols].to_numpy()
    print(f'INFO: The shape of the feature set is {x_features.shape}')
    y_labels = df_in[y_col].to_numpy().ravel()
    print(f'INFO: The shape of the labels is {y_labels.shape}')
    
    return x_features, y_labels

#-----------------------------------------------------------------------------#

def normalize_datasets(x_train:np.ndarray, x_test:np.ndarray):
    scaler = StandardScaler()
    scaler.fit(x_train)
    x_train_norm = scaler.transform(x_train)
    x_test_norm = scaler.transform(x_test)
    
    return x_train_norm, x_test_norm

#-----------------------------------------------------------------------------#

def transform_data_with_pca(N:int, x_train:np.ndarray, x_test:np.ndarray):
    print(f'INFO: running PCA for {N} components\n')
    pca_ = PCA(n_components=N)
    x_train_pca = pca_.fit_transform(x_train)
    x_test_pca = pca_.transform(x_test)
    
    return pca_, x_train_pca, x_test_pca

#-----------------------------------------------------------------------------#

def run_svm_nfl_predict(iX_train:np.ndarray, iY_train:np.ndarray,
                        iX_test:np.ndarray, iY_test:np.ndarray,
                        kernel_type:str='rbf', C_in:float=1.0, 
                        gamma_in='scale', degree_in:int=3):
    #
    # Expects the data to be normalized
    #
    
    try:
        clf = svm.SVC(kernel=kernel_type, cache_size=500, C=C_in, 
                      gamma=gamma_in, degree=degree_in)
        #clf = svm.SVC(kernel=kernel_type, cache_size=500, probability=True)
    except Exception as e:
        print(f"ERROR: failed to create an svm object for kernel {kernel_type}. Reason -> {e}\n")
        return
    
    # train the classifier
    clf.fit(iX_train, iY_train)
    
    # Run against the test set
    y_pred_test = clf.predict(iX_test)
    print(f'Test Set Accuracy: {metrics.accuracy_score(iY_test, y_pred_test)}')
    
    return clf

#-----------------------------------------------------------------------------#


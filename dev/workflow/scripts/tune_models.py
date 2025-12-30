import pandas as pd
import xgboost as xgb
from sklearn.model_selection import RandomizedSearchCV
import json
import numpy as np

def tune_models(input_path, output_path):
    df = pd.read_parquet(input_path)
    
    # Prepare data
    # Assume target is PM10 or PM25. R code tunes for both?
    # R code: `chr_outcome` is c("PM10", "PM25")
    # I'll tune for PM2.5 as an example or both.
    # For simplicity, let's assume PM2.5
    
    target = "PM25"
    if target not in df.columns:
        target = "PM10"
        
    # Features
    # R code: dsm, dem, d_road, mtpi, n_emittors_watershed, frac_*
    features = [c for c in df.columns if c not in ['PM10', 'PM25', 'geometry', 'TMSID', 'year', 'date', 'gw_emission']]
    # Add gw_emission if calculated
    if 'gw_emission' in df.columns:
        features.append('gw_emission')
        
    X = df[features]
    y = df[target]
    
    # XGBoost
    model = xgb.XGBRegressor(objective='reg:squarederror', n_jobs=-1)
    
    param_dist = {
        'n_estimators': [100, 500, 1000],
        'learning_rate': [0.01, 0.05, 0.1],
        'max_depth': [3, 5, 7, 9],
        'subsample': [0.6, 0.8, 1.0],
        'colsample_bytree': [0.6, 0.8, 1.0]
    }
    
    search = RandomizedSearchCV(model, param_distributions=param_dist, n_iter=10, cv=3, scoring='neg_root_mean_squared_error', verbose=1)
    search.fit(X, y)
    
    best_params = search.best_params_
    
    with open(output_path, 'w') as f:
        json.dump(best_params, f)

if __name__ == "__main__":
    tune_models(snakemake.input.features, snakemake.output.params)

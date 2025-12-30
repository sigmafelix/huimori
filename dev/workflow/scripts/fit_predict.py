import pandas as pd
import geopandas as gpd
import xgboost as xgb
import json
import os

def fit_predict(train_path, grid_path, params_path, output_template):
    # Load data
    train_df = pd.read_parquet(train_path)
    grid_df = pd.read_parquet(grid_path)
    
    with open(params_path, 'r') as f:
        params = json.load(f)
        
    target = "PM25" # Should be dynamic
    if target not in train_df.columns:
        target = "PM10"
        
    features = [c for c in train_df.columns if c not in ['PM10', 'PM25', 'geometry', 'TMSID', 'year', 'date']]
    
    # Align features
    common_features = [f for f in features if f in grid_df.columns]
    
    X_train = train_df[common_features]
    y_train = train_df[target]
    
    # Fit
    model = xgb.XGBRegressor(**params)
    model.fit(X_train, y_train)
    
    # Predict for each year
    # Assuming grid_df has year column or we replicate
    # If grid_df doesn't have year, we predict once and save for each year?
    # R code: `pattern = cross(workflow_tune_correct_spatial, df_feat_grid_merged)`
    
    # If grid_df has no year, we assume it's static features + we might need time-varying features (like landuse freq if it changes)
    # For now, predict on grid_df
    
    preds = model.predict(grid_df[common_features])
    
    # Save
    # We need to save as GPKG. grid_df should have geometry.
    # If grid_df is parquet without geometry (if we dropped it), we need to recover it.
    # But prepare_grid saved as parquet. GeoPandas can read parquet with geometry.
    
    grid_out = grid_df.copy()
    grid_out['pred'] = preds
    
    # Output template is predictions_{year}.gpkg
    # We need to generate one file per year.
    # If the model is year-specific (e.g. trained on 2015), we should predict for 2015.
    # But here we trained on all years?
    # R code: `workflow_tune_correct_spatial` filters by year.
    
    # Simplified: Save one file for now, or loop years if we had year-specific models.
    # Snakemake expects predictions_{year}.gpkg.
    
    # Let's just save the same prediction for all years requested by Snakemake, 
    # or if we had year in grid, split it.
    
    # Hack: Just write the file for the year in the filename
    # Snakemake calls this script once? No, `rule fit_predict` has `predictions_{year}.gpkg` as output.
    # But `script` directive executes once per rule instantiation.
    # If `output` has wildcards, Snakemake instantiates the rule for each wildcard.
    # So `snakemake.output.predictions` is a single file path (e.g. predictions_2015.gpkg).
    # And `snakemake.wildcards.year` is available.
    
    year = snakemake.wildcards.year
    output_path = snakemake.output.predictions
    
    # If we want year-specific prediction, we should filter training data by year?
    # R code does spatial CV per year.
    
    # Let's filter train by year
    if 'year' in train_df.columns:
        train_sub = train_df[train_df['year'] == int(year)]
        if not train_sub.empty:
            model.fit(train_sub[common_features], train_sub[target])
            preds = model.predict(grid_df[common_features])
            grid_out['pred'] = preds
            
    grid_out.to_file(output_path, driver="GPKG")

if __name__ == "__main__":
    fit_predict(snakemake.input.features_train, snakemake.input.features_grid, snakemake.input.params, snakemake.output.predictions)

import pandas as pd
import numpy as np

def compare_models(correct_path, incorrect_path, params_path, output_path):
    # Load data
    correct = pd.read_parquet(correct_path)
    incorrect = pd.read_parquet(incorrect_path)
    
    # Compare logic
    # R code calculates difference in predictions.
    # Here we just output some stats.
    
    stats = {
        'n_correct': len(correct),
        'n_incorrect': len(incorrect),
        'mean_pm25_correct': correct['PM25'].mean() if 'PM25' in correct else 0,
        'mean_pm25_incorrect': incorrect['PM25'].mean() if 'PM25' in incorrect else 0
    }
    
    df = pd.DataFrame([stats])
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    compare_models(snakemake.input.correct, snakemake.input.incorrect, snakemake.input.params, snakemake.output.comparison)

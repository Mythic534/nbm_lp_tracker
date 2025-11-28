from pathlib import Path
import pandas as pd

# === Config ===
DATA_DIR = Path("data")
ERROR_LOG = Path("errors/api_errors.log")
RESULTS_CSV = Path("results.csv")


results = []
for csv_file in DATA_DIR.iterdir():

    df = pd.read_csv(csv_file)
    df = df.iloc[:, 1:]
    results_series = df.mean()

    results.append(results_series)

df = pd.concat(results, axis=1).fillna(0)
df.columns = [f.stem for f in DATA_DIR.iterdir()]

df = df.round(4)
df = df.loc[df.sum(axis=1).sort_values(ascending=False).index]  # Sort by total score
df = df.loc[(df != 0).any(axis=1)]  # Remove users with 0 score due to rounding

df.to_csv(RESULTS_CSV)
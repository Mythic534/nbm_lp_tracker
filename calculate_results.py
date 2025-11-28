from pathlib import Path
import pandas as pd

# === Config ===
DATA_DIR = Path("data")
ERROR_LOG = Path("errors/api_errors.log")
RESULTS_CSV = Path("results.csv")

#pd.read_csv(DATA_DIR)
results = []
for csv_file in DATA_DIR.iterdir():

    df = pd.read_csv(csv_file)
    df = df.iloc[:, 1:]
    results_series = df.mean()

    results.append(results_series)

df = pd.concat(results, axis=1).fillna(0)
df.columns = [f.stem for f in DATA_DIR.iterdir()]

df.to_csv(RESULTS_CSV)
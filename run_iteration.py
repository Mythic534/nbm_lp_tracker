import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# === Config ===
DATA_DIR = Path("data")
ERROR_LOG = Path("errors/api_errors.log")
DATA_DIR.mkdir(parents=True, exist_ok=True)
ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)

pool_ids = [
    {"id": 294, "tokenA": "NBMFUS", "tokenB": "NBMACT"},
    {"id": 238, "tokenA": "NBMFUS", "tokenB": "WAX"},
    {"id": 292, "tokenA": "NBMCON", "tokenB": "NBMFUS"},
    {"id": 295, "tokenA": "NBMMIN", "tokenB": "NBMFUS"},
    {"id": 242, "tokenA": "NBMACT", "tokenB": "WAX"},
    {"id": 278, "tokenA": "NBMCON", "tokenB": "NBMACT"},
    {"id": 296, "tokenA": "NBMMIN", "tokenB": "NBMACT"},
    {"id": 245, "tokenA": "NBMMIN", "tokenB": "WAX"},
    {"id": 293, "tokenA": "NBMMIN", "tokenB": "NBMCON"},
    {"id": 246, "tokenA": "NBMCON", "tokenB": "WAX"},
]

def fetch_pool(pool):
    """Return raw API response"""

    pool_id = pool.get("id")
    url = f"https://alcor.exchange/api/v2/swap/pools/{pool_id}/positions"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    raw_pool_data = requests.get(url, headers=headers)

    # Alcor API can be unreliable
    if raw_pool_data.status_code == 504:
        time.sleep(10)
        fetch_pool(pool)

    if raw_pool_data.status_code != 200:
        raise Exception(f"API request failed with status code {raw_pool_data.status_code}")
    
    return raw_pool_data


def filter_snapshot(raw_pool_data):
    """Total USD value of a wallet's position is compared to all users who are in range, and converted into the wallet's share of the active pool"""

    wallet_value = []

    for user in raw_pool_data:

        wallet = user.get("owner")
        usd_value = user.get("totalValue")
        in_range = user.get("inRange")

        if usd_value and in_range:
            wallet_value.append([wallet, usd_value])

    # Combine duplicate wallets
    df = pd.DataFrame(wallet_value, columns=["wallet", "usd_value"])
    df = df.groupby("wallet", as_index=False).sum()

    total = df["usd_value"].sum()
    df["share_percent"] = (df["usd_value"] / total * 100).round(4)
    return df[["wallet", "share_percent"]]


def write_snapshot(pool, wallet_shares):

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    pool_file = DATA_DIR / f"{pool.get('tokenA')}_{pool.get('tokenB')}.csv"

    df_new = wallet_shares.copy()
    df_new["timestamp"] = timestamp
    df_pivot = df_new.pivot(index="timestamp", columns="wallet", values="share_percent")

    if pool_file.exists():
        df_existing = pd.read_csv(pool_file, index_col=0)
        df_existing.index.name = "timestamp"
        df_combined = pd.concat([df_existing, df_pivot], axis=0, sort=False)
    else:
        df_combined = df_pivot

    df_combined = df_combined.fillna(0)
    df_combined.to_csv(pool_file)


def log_error(pool, message):

    entry = {"timestamp": datetime.now(timezone.utc).isoformat(), "pool": f"{pool.get('tokenA')}_{pool.get('tokenB')}", "error": message}
    with ERROR_LOG.open("a") as f:
        f.write(f"{entry}\n")


if __name__ == "__main__":
    
    for pool in pool_ids:
        try:
            raw_pool_data = fetch_pool(pool)
            if not raw_pool_data:
                log_error(pool, "API returned no data")
                
            wallet_shares = filter_snapshot(raw_pool_data.json())
            write_snapshot(pool, wallet_shares)

            time.sleep(1)  # Be nice to the API

        except Exception as e:
            log_error(pool, str(e))

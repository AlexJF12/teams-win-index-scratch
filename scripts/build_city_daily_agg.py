from __future__ import annotations

from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"
ALL_LONG = OUTPUTS_DIR / "all_long.csv"
CITY_DAILY_OUT = OUTPUTS_DIR / "city_daily_7d.csv"


def main() -> None:
    if not ALL_LONG.exists():
        raise FileNotFoundError(f"Missing input: {ALL_LONG}")

    df = pd.read_csv(ALL_LONG)
    required = {"date", "City", "index_score"}
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(f"all_long.csv missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "City"])

    # Filter to 2022 and later
    cutoff = pd.Timestamp("2022-01-01")
    df = df[df["date"] >= cutoff]

    # Daily aggregation per city
    daily = (
        df.groupby(["City", "date"], as_index=False)
        .agg(index_sum=("index_score", "sum"), games=("index_score", "count"))
    )

    # Build 7-day rolling sums over calendar days by reindexing per city
    out_frames = []
    for city, g in daily.groupby("City"):
        g = g.set_index("date").sort_index()
        full_range = pd.date_range(g.index.min(), g.index.max(), freq="D")
        g = g.reindex(full_range, fill_value=0)
        g.index.name = "date"
        g["City"] = city
        g["index_sum_7d"] = g["index_sum"].rolling(window=7, min_periods=1).sum()
        g["games_7d"] = g["games"].rolling(window=7, min_periods=1).sum()
        out_frames.append(g.reset_index())

    result = (
        pd.concat(out_frames, ignore_index=True)
        if out_frames
        else pd.DataFrame(columns=["date", "City", "index_sum", "games", "index_sum_7d", "games_7d"])
    )
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    result = result[["date", "City", "index_sum", "games", "index_sum_7d", "games_7d"]].sort_values(["City", "date"]) 

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(CITY_DAILY_OUT, index=False)
    print(f"Wrote city daily aggregates → {CITY_DAILY_OUT} ({len(result)} rows)")


if __name__ == "__main__":
    main()

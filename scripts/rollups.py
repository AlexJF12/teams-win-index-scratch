from __future__ import annotations

from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUTS_DIR = DATA_DIR / "outputs"


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def compute_rollups() -> None:
    master = safe_read_csv(PROCESSED_DIR / "team_game_results.csv")
    teams = safe_read_csv(PROCESSED_DIR / "teams.csv")

    if master.empty or teams.empty:
        print("No rollups computed (missing master or teams).")
        return

    # Team weekly/monthly
    team_cols = [
        "league", "team_id", "week_start", "month_start",
        "index_score", "weighted_score"
    ]
    df = master[team_cols].copy()

    team_weekly = (
        df.groupby(["league", "team_id", "week_start"], as_index=False)
        .agg(index_score_sum=("index_score", "sum"), weighted_score_sum=("weighted_score", "sum"), games=("index_score", "count"))
    )
    team_monthly = (
        df.groupby(["league", "team_id", "month_start"], as_index=False)
        .agg(index_score_sum=("index_score", "sum"), weighted_score_sum=("weighted_score", "sum"), games=("index_score", "count"))
    )

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    team_weekly.to_csv(OUTPUTS_DIR / "team_rollup_weekly.csv", index=False)
    team_monthly.to_csv(OUTPUTS_DIR / "team_rollup_monthly.csv", index=False)

    # City weekly/monthly (map team -> city)
    team_to_city = teams.set_index("team_id")["city_id"].to_dict()
    df["city_id"] = df["team_id"].map(team_to_city)
    df = df.dropna(subset=["city_id"])  # drop teams with no mapping

    city_weekly = (
        df.groupby(["city_id", "week_start"], as_index=False)
        .agg(index_score_sum=("index_score", "sum"), weighted_score_sum=("weighted_score", "sum"), games=("index_score", "count"))
    )
    city_monthly = (
        df.groupby(["city_id", "month_start"], as_index=False)
        .agg(index_score_sum=("index_score", "sum"), weighted_score_sum=("weighted_score", "sum"), games=("index_score", "count"))
    )

    city_weekly.to_csv(OUTPUTS_DIR / "city_rollup_weekly.csv", index=False)
    city_monthly.to_csv(OUTPUTS_DIR / "city_rollup_monthly.csv", index=False)

    print("Wrote weekly/monthly rollups for teams and cities → data/outputs")


if __name__ == "__main__":
    compute_rollups()

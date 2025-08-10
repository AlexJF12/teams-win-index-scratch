from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from datetime import datetime
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"
ALL_LONG_DEFAULT = OUTPUTS_DIR / "all_long.csv"


def compute_selected_teams_monthly(
    all_long_path: str | Path,
    team_by_league: Dict[str, str],
) -> pd.DataFrame:
    """Aggregate monthly scores for a selected set of teams (one per league).

    Parameters
    - all_long_path: path to all_long.csv produced by the league transformers + concat step
    - team_by_league: dict mapping each league to the selected team identifier used in that league's long CSV
        Expected keys: 'nhl', 'mlb', 'nba', 'nfl'
        Examples:
          nhl: 'New York Rangers' (full team name)
          mlb: 'NYN' (retro team code per MLB dataset)
          nba: 'NYK' (team abbreviation)
          nfl: 'New York Giants' (full team name)

    Returns
    - DataFrame with columns: month_end (YYYY-MM-DD), month (YYYY-MM-01), total_index_score, games
    """
    path = Path(all_long_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    df = pd.read_csv(path)
    required = {"date", "team", "index_score", "month", "league"}
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(f"all_long.csv missing columns: {sorted(missing)}")

    # Normalize
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = pd.to_datetime(df["month"], errors="coerce")  # first of month per our pipeline
    df = df.dropna(subset=["date", "month", "team", "league"]).copy()
    df["league"] = df["league"].str.lower()

    # Validate selections
    expected_keys = {"nhl", "mlb", "nba", "nfl"}
    missing_keys = expected_keys.difference(set(k.lower() for k in team_by_league.keys()))
    if missing_keys:
        raise ValueError(f"team_by_league missing required leagues: {sorted(missing_keys)}")

    # Filter to selected teams per league
    # Normalize dict keys to lower-case leagues
    selection = {k.lower(): v for k, v in team_by_league.items()}
    mask = False
    for lg, team in selection.items():
        lg_mask = (df["league"] == lg) & (df["team"].astype(str) == str(team))
        mask = lg_mask if mask is False else (mask | lg_mask)
    picked = df[mask].copy()

    if picked.empty:
        # Return empty structure
        return pd.DataFrame(columns=["month_end", "month", "total_index_score", "games"])  # type: ignore

    # Month end (for display); month col is first day
    picked["month_end"] = picked["month"].dt.to_period("M").dt.end_time.dt.normalize()

    monthly = (
        picked.groupby("month", as_index=False)
        .agg(total_index_score=("index_score", "sum"), games=("index_score", "count"))
        .sort_values("month")
    )
    monthly["month_end"] = monthly["month"].dt.to_period("M").dt.end_time.dt.normalize()
    monthly["month"] = monthly["month"].dt.strftime("%Y-%m-01")
    monthly["month_end"] = monthly["month_end"].dt.strftime("%Y-%m-%d")
    monthly = monthly[["month_end", "month", "total_index_score", "games"]]
    return monthly


def _slugify_token(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Monthly rollup for selected teams (one per league)")
    parser.add_argument("--nhl", required=True, help="NHL team (e.g., 'New York Rangers')")
    parser.add_argument("--mlb", required=True, help="MLB team code (e.g., 'NYN')")
    parser.add_argument("--nba", required=True, help="NBA team abbreviation (e.g., 'NYK')")
    parser.add_argument("--nfl", required=True, help="NFL team (e.g., 'New York Giants')")
    parser.add_argument("--input", default=str(ALL_LONG_DEFAULT), help="Path to all_long.csv")
    parser.add_argument("--out", default="", help="Optional output CSV path")
    args = parser.parse_args()

    teams = {"nhl": args.nhl, "mlb": args.mlb, "nba": args.nba, "nfl": args.nfl}
    out_df = compute_selected_teams_monthly(args.input, teams)
    print(f"Monthly rows: {len(out_df)}")

    out_path: Path
    if args.out:
        out_path = Path(args.out)
    else:
        # Auto-generate filename with teams and date
        today = datetime.now().strftime("%Y%m%d")
        nhl_tok = _slugify_token(args.nhl)
        mlb_tok = _slugify_token(args.mlb)
        nba_tok = _slugify_token(args.nba)
        nfl_tok = _slugify_token(args.nfl)
        filename = f"monthly_{today}_nhl-{nhl_tok}_mlb-{mlb_tok}_nba-{nba_tok}_nfl-{nfl_tok}.csv"
        out_path = OUTPUTS_DIR / filename

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")

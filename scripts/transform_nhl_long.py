from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def transform_nhl_long(df: pd.DataFrame) -> pd.DataFrame:
    """Transform NHL games wide CSV to long format (two rows per game) with Win/Loss,
    then project to minimal schema: date, team, index_score, month, league.

    Expected input columns (case-sensitive):
      - Date
      - Away, AwayGoals
      - Home, HomeGoals
      - Type (e.g., "Regular Season", contains "Playoff" for playoffs)

    Output columns:
      - date (YYYY-MM-DD)
      - team (team name as in source)
      - index_score (+1 win, -1 loss, 0 unknown)
      - month (first day of month, YYYY-MM-01)
      - league (constant 'nhl')
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "team",
                "index_score",
                "month",
                "league",
            ]
        )

    # Normalize columns
    required = ["Date", "Away", "AwayGoals", "Home", "HomeGoals", "Type"]
    for col in required:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' in NHL dataframe")

    rows = []
    for _, r in df.iterrows():
        date = str(r["Date"]).split(" ")[0]
        away = str(r["Away"]).strip()
        home = str(r["Home"]).strip()
        try:
            away_goals = int(float(r["AwayGoals"])) if pd.notna(r["AwayGoals"]) else None
        except Exception:
            away_goals = None
        try:
            home_goals = int(float(r["HomeGoals"])) if pd.notna(r["HomeGoals"]) else None
        except Exception:
            home_goals = None

        if not away or not home:
            continue

        if away_goals is None or home_goals is None:
            away_idx = 0
            home_idx = 0
        else:
            if away_goals > home_goals:
                away_idx = 1
                home_idx = -1
            elif home_goals > away_goals:
                away_idx = -1
                home_idx = 1
            else:
                away_idx = 0
                home_idx = 0

        rows.append(
            {
                "date": date,
                "team": away,
                "index_score": away_idx,
                "league": "nhl",
            }
        )
        rows.append(
            {
                "date": date,
                "team": home,
                "index_score": home_idx,
                "league": "nhl",
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
        out["month"] = pd.to_datetime(out["date"]).dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-01")
        out = out[["date", "team", "index_score", "month", "league"]].sort_values(["date", "team"]).reset_index(drop=True)
    return out


def from_csv_to_long(input_path: str | Path, output_path: Optional[str | Path] = None) -> pd.DataFrame:
    """Convenience: load from CSV, transform to long, optionally write to CSV, and return the dataframe."""
    input_path = Path(input_path)
    df = pd.read_csv(input_path)
    long_df = transform_nhl_long(df)
    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        long_df.to_csv(output_path, index=False)
    return long_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transform NHL wide CSV to long per-team format (minimal schema)")
    parser.add_argument("input", help="Path to nhl_season_games_2018_2025.csv")
    parser.add_argument("--out", help="Optional output CSV path", default="")
    args = parser.parse_args()

    out_df = from_csv_to_long(args.input, args.out or None)
    print(f"Transformed rows: {len(out_df)}")
    if args.out:
        print(f"Wrote: {args.out}")

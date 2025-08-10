from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def _format_out(df: pd.DataFrame, league: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "team", "index_score", "month", "league"])  # type: ignore
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-01")
    df["league"] = league
    return df[["date", "team", "index_score", "month", "league"]].sort_values(["date", "team"]).reset_index(drop=True)


def transform_nhl_long(df: pd.DataFrame) -> pd.DataFrame:
    """NHL: wide to long (two rows per game). Input columns: Date, Away, AwayGoals, Home, HomeGoals, Type.
    Output minimal schema: date, team, index_score, month, league('nhl')."""
    if df.empty:
        return _format_out(pd.DataFrame(columns=["date", "team", "index_score"]), "nhl")

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
        rows.append({"date": date, "team": away, "index_score": away_idx})
        rows.append({"date": date, "team": home, "index_score": home_idx})

    out = pd.DataFrame(rows)
    return _format_out(out, "nhl")


def transform_mlb_long(df: pd.DataFrame) -> pd.DataFrame:
    """MLB: from MLB2020-2024GameInfo_small schema.
    Input columns: Date (yyyymmdd), VT (visitor code), HT (home code), Game Winner (1 visitor wins, 0 home wins).
    Output minimal schema: date, team, index_score, month, league('mlb')."""
    if df.empty:
        return _format_out(pd.DataFrame(columns=["date", "team", "index_score"]), "mlb")

    required = ["Date", "VT", "HT", "Game Winner"]
    for col in required:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' in MLB dataframe")

    rows = []
    for _, r in df.iterrows():
        date_raw = str(r["Date"]).strip()
        date = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}" if len(date_raw) == 8 else date_raw
        vt = str(r["VT"]).strip()
        ht = str(r["HT"]).strip()
        try:
            winner = int(r["Game Winner"]) if pd.notna(r["Game Winner"]) else None
        except Exception:
            winner = None
        if not vt or not ht:
            continue
        if winner is None:
            vt_idx = 0
            ht_idx = 0
        else:
            if winner == 1:
                vt_idx = 1
                ht_idx = -1
            elif winner == 0:
                vt_idx = -1
                ht_idx = 1
            else:
                vt_idx = 0
                ht_idx = 0
        rows.append({"date": date, "team": vt, "index_score": vt_idx})
        rows.append({"date": date, "team": ht, "index_score": ht_idx})

    out = pd.DataFrame(rows)
    return _format_out(out, "mlb")


def transform_nba_long(df: pd.DataFrame) -> pd.DataFrame:
    """NBA: from regular season totals schema.
    Input columns: TEAM_ABBREVIATION, GAME_DATE, WL (W/L).
    Output minimal schema: date, team, index_score, month, league('nba')."""
    if df.empty:
        return _format_out(pd.DataFrame(columns=["date", "team", "index_score"]), "nba")

    required = ["TEAM_ABBREVIATION", "GAME_DATE", "WL"]
    for col in required:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' in NBA dataframe")

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d"),
            "team": df["TEAM_ABBREVIATION"].astype(str),
            "index_score": df["WL"].map({"W": 1, "L": -1}).fillna(0).astype(int),
        }
    )
    return _format_out(out, "nba")


def transform_nfl_long(df: pd.DataFrame) -> pd.DataFrame:
    """NFL: from nfl_scores_2015_2025 schema.
    Input columns: schedule_date, team_home, score_home, team_away, score_away (score_away column name present in CSV as 'score_away').
    Output minimal schema: date, team, index_score, month, league('nfl')."""
    if df.empty:
        return _format_out(pd.DataFrame(columns=["date", "team", "index_score"]), "nfl")

    required = ["schedule_date", "team_home", "score_home", "team_away", "score_away"]
    for col in required:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' in NFL dataframe")

    rows = []
    for _, r in df.iterrows():
        date_raw = str(r["schedule_date"]).strip()
        # Normalize date to YYYY-MM-DD via pandas
        date = pd.to_datetime(date_raw, errors="coerce").strftime("%Y-%m-%d") if date_raw else ""
        home = str(r["team_home"]).strip()
        away = str(r["team_away"]).strip()
        try:
            home_pts = int(r["score_home"]) if pd.notna(r["score_home"]) else None
        except Exception:
            home_pts = None
        try:
            away_pts = int(r["score_away"]) if pd.notna(r["score_away"]) else None
        except Exception:
            away_pts = None

        if not home or not away:
            continue

        if home_pts is None or away_pts is None:
            home_idx = 0
            away_idx = 0
        else:
            if home_pts > away_pts:
                home_idx = 1
                away_idx = -1
            elif away_pts > home_pts:
                home_idx = -1
                away_idx = 1
            else:
                home_idx = 0
                away_idx = 0

        rows.append({"date": date, "team": home, "index_score": home_idx})
        rows.append({"date": date, "team": away, "index_score": away_idx})

    out = pd.DataFrame(rows)
    return _format_out(out, "nfl")


def from_csv_to_long(input_path: str | Path, league: str, output_path: Optional[str | Path] = None) -> pd.DataFrame:
    """Load a league CSV, transform to long minimal schema, optionally write CSV, and return the DataFrame.

    league one of: 'nhl', 'mlb', 'nba'
    """
    input_path = Path(input_path)
    df = pd.read_csv(input_path)
    league_l = league.strip().lower()
    if league_l == "nhl":
        long_df = transform_nhl_long(df)
    elif league_l == "mlb":
        long_df = transform_mlb_long(df)
    elif league_l == "nba":
        long_df = transform_nba_long(df)
    elif league_l == "nfl":
        long_df = transform_nfl_long(df)
    else:
        raise ValueError(f"Unsupported league: {league}")

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        long_df.to_csv(output_path, index=False)
    return long_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transform league CSV to long per-team format (minimal schema)")
    parser.add_argument("league", choices=["nhl", "mlb", "nba", "nfl"], help="League identifier")
    parser.add_argument("input", help="Path to source CSV for the league")
    parser.add_argument("--out", help="Optional output CSV path", default="")
    args = parser.parse_args()

    out_df = from_csv_to_long(args.input, args.league, args.out or None)
    print(f"Transformed rows: {len(out_df)}")
    if args.out:
        print(f"Wrote: {args.out}")

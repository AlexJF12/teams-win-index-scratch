from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
CONFIG_DIR = REPO_ROOT / "config"


def load_scoring_weights() -> dict[str, int]:
    scoring_path = CONFIG_DIR / "scoring.json"
    if not scoring_path.exists():
        return {
            "regular_season_win": 1,
            "regular_season_loss": -1,
            "playoff_win": 3,
            "playoff_loss": -3,
        }
    return json.loads(scoring_path.read_text(encoding="utf-8"))


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def compute_master() -> pd.DataFrame:
    games = safe_read_csv(PROCESSED_DIR / "games.csv")
    if games.empty:
        return pd.DataFrame(columns=[
            "game_id", "date", "league", "season_type", "team_id", "opponent_team_id",
            "is_home", "team_score", "opponent_score", "result", "index_score", "weighted_score",
            "week_start", "month_start"
        ])

    required = {
        "game_id", "date", "league", "season_type", "home_team_id", "away_team_id", "home_score", "away_score", "winning_team_id"
    }
    missing = [c for c in required if c not in games.columns]
    if missing:
        raise ValueError(f"games.csv missing columns: {missing}")

    weights = load_scoring_weights()

    rows = []
    for _, g in games.iterrows():
        date_str = str(g["date"])[:10]
        league = str(g["league"]).lower()
        season_type = str(g["season_type"]).lower()
        home_id = str(g["home_team_id"]) 
        away_id = str(g["away_team_id"]) 
        try:
            home_score = int(g["home_score"]) if pd.notna(g["home_score"]) and str(g["home_score"]).strip() != "" else None
            away_score = int(g["away_score"]) if pd.notna(g["away_score"]) and str(g["away_score"]).strip() != "" else None
        except Exception:
            home_score = None
            away_score = None
        winning_team_id = str(g["winning_team_id"]) if pd.notna(g["winning_team_id"]) and str(g["winning_team_id"]).strip() != "" else None

        # Determine base results
        if home_score is not None and away_score is not None and winning_team_id is not None:
            home_result = "W" if winning_team_id == home_id else "L"
            away_result = "W" if winning_team_id == away_id else "L"
            base_home = 1 if home_result == "W" else -1
            base_away = 1 if away_result == "W" else -1
        else:
            # If incomplete, mark unknown result and zero score
            home_result = ""
            away_result = ""
            base_home = 0
            base_away = 0

        # Weighted per season type
        if base_home == 1:
            weight_home = weights.get("playoff_win" if season_type == "playoff" else "regular_season_win", 1)
        elif base_home == -1:
            weight_home = weights.get("playoff_loss" if season_type == "playoff" else "regular_season_loss", -1)
        else:
            weight_home = 0
        weighted_home = base_home * abs(weight_home)

        if base_away == 1:
            weight_away = weights.get("playoff_win" if season_type == "playoff" else "regular_season_win", 1)
        elif base_away == -1:
            weight_away = weights.get("playoff_loss" if season_type == "playoff" else "regular_season_loss", -1)
        else:
            weight_away = 0
        weighted_away = base_away * abs(weight_away)

        rows.append({
            "game_id": g["game_id"],
            "date": date_str,
            "league": league,
            "season_type": season_type,
            "team_id": home_id,
            "opponent_team_id": away_id,
            "is_home": True,
            "team_score": home_score,
            "opponent_score": away_score,
            "result": home_result,
            "index_score": base_home,
            "weighted_score": weighted_home,
        })
        rows.append({
            "game_id": g["game_id"],
            "date": date_str,
            "league": league,
            "season_type": season_type,
            "team_id": away_id,
            "opponent_team_id": home_id,
            "is_home": False,
            "team_score": away_score,
            "opponent_score": home_score,
            "result": away_result,
            "index_score": base_away,
            "weighted_score": weighted_away,
        })

    master = pd.DataFrame(rows)

    # Derive week_start (ISO week starting Monday) and month_start
    if not master.empty:
        master["date_ts"] = pd.to_datetime(master["date"], errors="coerce")
        # Week start (Monday)
        master["week_start"] = master["date_ts"].dt.to_period("W-MON").dt.start_time.dt.date.astype(str)
        # Month start
        master["month_start"] = master["date_ts"].dt.to_period("M").dt.start_time.dt.date.astype(str)
        master = master.drop(columns=["date_ts"])

    master = master.sort_values(["date", "league", "game_id", "team_id"]).reset_index(drop=True)
    return master


def main() -> None:
    master = compute_master()
    out_path = PROCESSED_DIR / "team_game_results.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(out_path, index=False)
    print(f"Computed team_game_results.csv with {len(master)} rows → {out_path}")


if __name__ == "__main__":
    main()

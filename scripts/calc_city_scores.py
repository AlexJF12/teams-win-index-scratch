from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUTS_DIR = DATA_DIR / "outputs"
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
    # Strip whitespace from column names
    df.columns = [c.strip() for c in df.columns]
    return df


def compute_city_scores() -> tuple[pd.DataFrame, pd.DataFrame]:
    team_games = safe_read_csv(PROCESSED_DIR / "team_game_results.csv")
    teams = safe_read_csv(PROCESSED_DIR / "teams.csv")
    cities = safe_read_csv(PROCESSED_DIR / "cities.csv")

    if team_games.empty or teams.empty or cities.empty:
        empty_cols = [
            "date", "city_id", "city_name", "score", "wins", "losses", "playoff_wins", "playoff_losses"
        ]
        return pd.DataFrame(columns=empty_cols), pd.DataFrame(columns=empty_cols)

    team_to_city = teams.set_index("team_id")["city_id"].to_dict()
    city_id_to_name = cities.set_index("city_id")["city_name"].to_dict()

    # Map team rows to city and aggregate by date+city
    team_games = team_games.copy()
    team_games["city_id"] = team_games["team_id"].map(team_to_city)
    team_games = team_games.dropna(subset=["city_id"])  # drop teams without mapping

    # Wins/losses split by season_type
    team_games["win_flag"] = (team_games["result"] == "W").astype(int)
    team_games["loss_flag"] = (team_games["result"] == "L").astype(int)

    team_games["playoff_win_flag"] = ((team_games["result"] == "W") & (team_games["season_type"].str.lower() == "playoff")).astype(int)
    team_games["playoff_loss_flag"] = ((team_games["result"] == "L") & (team_games["season_type"].str.lower() == "playoff")).astype(int)

    grouped = (
        team_games.groupby(["date", "city_id"], as_index=False)
        .agg(
            wins=("win_flag", "sum"),
            losses=("loss_flag", "sum"),
            playoff_wins=("playoff_win_flag", "sum"),
            playoff_losses=("playoff_loss_flag", "sum"),
            score=("weighted_score", "sum"),
        )
    )

    grouped["city_name"] = grouped["city_id"].map(city_id_to_name).fillna("")
    cols = ["date", "city_id", "city_name", "score", "wins", "losses", "playoff_wins", "playoff_losses"]
    grouped = grouped[cols].sort_values(["date", "score"], ascending=[True, False])

    # Latest snapshot
    latest = grouped[grouped["date"] == grouped["date"].max()] if not grouped.empty else grouped

    return grouped, latest


def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    all_scores, latest = compute_city_scores()
    all_scores.to_csv(OUTPUTS_DIR / "city_scores.csv", index=False)
    latest.to_csv(OUTPUTS_DIR / "city_scores_latest.csv", index=False)
    print(
        f"Computed scores. Rows: all={len(all_scores)}, latest={len(latest)} → {OUTPUTS_DIR}"
    )


if __name__ == "__main__":
    main() 
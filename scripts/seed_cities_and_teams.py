import csv
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"


def ensure_directories_exist() -> None:
    for directory in [DATA_DIR, PROCESSED_DIR, DATA_DIR / "daily", DATA_DIR / "outputs"]:
        directory.mkdir(parents=True, exist_ok=True)


def write_csv_if_missing(path: Path, headers: list[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)


def main() -> None:
    ensure_directories_exist()

    # Canonical cities
    write_csv_if_missing(
        PROCESSED_DIR / "cities.csv",
        ["city_id", "city_name", "state", "country", "slug"],
    )

    # Canonical teams
    write_csv_if_missing(
        PROCESSED_DIR / "teams.csv",
        [
            "team_id",
            "team_name",
            "league",
            "city_id",
            "city_name",
            "start_date",
            "end_date",
            "alt_names",
        ],
    )

    # Normalized games (one row per game)
    write_csv_if_missing(
        PROCESSED_DIR / "games.csv",
        [
            "game_id",
            "date",
            "league",
            "season_type",
            "home_team_id",
            "away_team_id",
            "home_score",
            "away_score",
            "winning_team_id",
        ],
    )

    # Per-team per-game master table
    write_csv_if_missing(
        PROCESSED_DIR / "team_game_results.csv",
        [
            "game_id",
            "date",
            "league",
            "season_type",
            "team_id",
            "opponent_team_id",
            "is_home",
            "team_score",
            "opponent_score",
            "result",           # W or L
            "index_score",      # +1/-1 base
            "weighted_score",   # weighted by season_type from config
        ],
    )

    # Outputs
    write_csv_if_missing(
        DATA_DIR / "outputs" / "city_scores.csv",
        [
            "date",
            "city_id",
            "city_name",
            "score",
            "wins",
            "losses",
            "playoff_wins",
            "playoff_losses",
        ],
    )

    write_csv_if_missing(
        DATA_DIR / "outputs" / "city_scores_latest.csv",
        ["date", "city_id", "city_name", "score", "wins", "losses", "playoff_wins", "playoff_losses"],
    )

    print("Seed complete. CSV scaffolding ensured under data/processed and data/outputs.")


if __name__ == "__main__":
    main() 
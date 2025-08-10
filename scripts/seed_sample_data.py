from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"


def csv_has_data(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
        return len(rows) > 1


def write_rows(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        return
    # Append
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def seed_demo() -> None:
    cities_path = PROCESSED_DIR / "cities.csv"
    teams_path = PROCESSED_DIR / "teams.csv"
    games_path = PROCESSED_DIR / "games.csv"

    # Only seed if there is no data yet
    if csv_has_data(teams_path) or csv_has_data(cities_path) or csv_has_data(games_path):
        print("Sample data already present. Skipping demo seed.")
        return

    # Cities (ids are short slugs)
    city_headers = ["city_id", "city_name", "state", "country", "slug"]
    city_rows = [
        ["bos", "Boston", "MA", "USA", "boston"],
        ["la", "Los Angeles", "CA", "USA", "los-angeles"],
        ["nyc", "New York", "NY", "USA", "new-york"],
    ]
    write_rows(cities_path, city_headers, city_rows)

    # Teams (ids prefixed by league)
    team_headers = [
        "team_id",
        "team_name",
        "league",
        "city_id",
        "city_name",
        "start_date",
        "end_date",
        "alt_names",
    ]
    team_rows = [
        ["nba_bos", "Boston Celtics", "nba", "bos", "Boston", "1900-01-01", "", "Celtics"],
        ["nba_lal", "Los Angeles Lakers", "nba", "la", "Los Angeles", "1900-01-01", "", "Lakers"],
        ["nba_nyk", "New York Knicks", "nba", "nyc", "New York", "1900-01-01", "", "Knicks"],
    ]
    write_rows(teams_path, team_headers, team_rows)

    # A couple of recent games (yesterday and two days ago)
    today = datetime.utcnow().date()
    d1 = today - timedelta(days=2)
    d2 = today - timedelta(days=1)

    game_headers = [
        "game_id",
        "date",
        "league",
        "season_type",
        "home_team_id",
        "away_team_id",
        "home_score",
        "away_score",
        "winning_team_id",
    ]
    game_rows = [
        [f"nba_{d1}_bos_lal", str(d1), "nba", "regular", "nba_bos", "nba_lal", "110", "102", "nba_bos"],
        [f"nba_{d2}_nyk_bos", str(d2), "nba", "regular", "nba_nyk", "nba_bos", "95", "108", "nba_bos"],
    ]
    write_rows(games_path, game_headers, game_rows)

    print("Seeded demo cities, teams, and games.")


if __name__ == "__main__":
    seed_demo()

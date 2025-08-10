import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

from dateutil import tz

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
DAILY_DIR = DATA_DIR / "daily"
PROCESSED_DIR = DATA_DIR / "processed"


HEADERS = [
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


def get_yesterday_date_str() -> str:
    # Use US/Eastern as a default sports-centric timezone
    eastern = tz.gettz("US/Eastern")
    today_et = datetime.now(tz=eastern)
    yesterday_et = today_et - timedelta(days=1)
    return yesterday_et.strftime("%Y-%m-%d")


def ensure_empty_snapshot(date_str: str) -> Path:
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_path = DAILY_DIR / f"{date_str}.csv"
    if not snapshot_path.exists():
        with snapshot_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)
    return snapshot_path


def append_snapshot_to_games(snapshot_path: Path) -> None:
    """Append daily rows to processed/games.csv if any rows exist beyond header."""
    with snapshot_path.open("r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if len(rows) <= 1:
        print("No daily rows to append (empty snapshot).")
        return

    games_path = PROCESSED_DIR / "games.csv"
    existing_ids = set()
    with games_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            existing_ids.add(r["game_id"])  # skip duplicates

    with games_path.open("a", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        for row in rows[1:]:
            if row and row[0] not in existing_ids:
                writer.writerow(row)


def main() -> None:
    date_str = get_yesterday_date_str()
    snapshot_path = ensure_empty_snapshot(date_str)

    # Placeholder: integrate TheSportsDB fetch here.
    # Keep pipeline green without API by leaving snapshot empty when not configured.
    if os.getenv("THESPORTSDB_API_KEY"):
        print("THESPORTSDB_API_KEY provided; implement API fetch to populate snapshot.")
    else:
        print("No THESPORTSDB_API_KEY provided; writing/keeping empty snapshot.")

    append_snapshot_to_games(snapshot_path)
    print(f"Daily snapshot processed for {date_str} → {snapshot_path}")


if __name__ == "__main__":
    main() 
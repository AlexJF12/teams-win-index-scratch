from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
CSV_PATH = DATA_DIR / "team_city_map.csv"
JSON_PATH = DATA_DIR / "team_city_map.json"


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing {CSV_PATH}. Generate it first (e.g., run scripts/concat_leagues_with_city.py)")

    df = pd.read_csv(CSV_PATH)
    required = {"league", "team", "city"}
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(f"team_city_map.csv missing columns: {sorted(missing)}")

    mapping: dict[str, dict[str, str]] = {}
    for league, g in df.groupby("league"):
        league_key = str(league).lower()
        league_map = {str(r["team"]): str(r["city"]) for _, r in g.iterrows()}
        mapping[league_key] = league_map

    JSON_PATH.write_text(json.dumps(mapping, indent=2), encoding="utf-8")
    print(f"Wrote team-to-city JSON mapping → {JSON_PATH}")


if __name__ == "__main__":
    main()

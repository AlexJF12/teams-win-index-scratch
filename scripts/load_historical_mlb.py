from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

MLB_GAMES_PATH = DATA_DIR / "MLB2020-2024GameInfo_small.csv"
CURRENT_NAMES_PATH = DATA_DIR / "CurrentNames.csv"


def slugify_city(city: str, state: str | None = None) -> str:
    base = (city or "").strip().lower().replace(" ", "-").replace("/", "-")
    if state:
        return f"{base}-{state.strip().lower()}"
    return base


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def read_current_names_mapping() -> Dict[str, Tuple[str, str]]:
    """Return mapping retro_code -> (city, state) using the latest/current row for each code.
    The CurrentNames.csv has no header; columns per sample rows:
    0: current code, 1: retro code used in game CSV, 4: City, 5: Nickname? then later columns include City, State at the end.
    We'll parse by position from the right: last two are city, state.
    """
    mapping: Dict[str, Tuple[str, str]] = {}
    if not CURRENT_NAMES_PATH.exists():
        return mapping

    with CURRENT_NAMES_PATH.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            # Defensive: ensure we have at least city,state at the end
            if len(row) < 2:
                continue
            retro = row[1].strip()
            city = row[-2].strip() if len(row) >= 2 else ""
            state = row[-1].strip() if len(row) >= 1 else ""
            if retro:
                mapping[retro] = (city, state)
    return mapping


def load_games_and_entities() -> None:
    if not MLB_GAMES_PATH.exists():
        print(f"Missing MLB file: {MLB_GAMES_PATH}")
        return

    games_csv = pd.read_csv(MLB_GAMES_PATH)
    games_csv.columns = [c.strip() for c in games_csv.columns]

    # Build teams set from VT and HT columns (retro codes)
    retro_codes = set(games_csv["VT"].unique()).union(set(games_csv["HT"].unique()))

    # Map retro code -> city/state
    retro_to_city_state = read_current_names_mapping()

    # Load existing processed entities
    processed_teams = safe_read_csv(PROCESSED_DIR / "teams.csv")
    processed_cities = safe_read_csv(PROCESSED_DIR / "cities.csv")
    processed_games = safe_read_csv(PROCESSED_DIR / "games.csv")

    # Ensure columns
    if processed_teams.empty:
        processed_teams = pd.DataFrame(
            columns=[
                "team_id",
                "team_name",
                "league",
                "city_id",
                "city_name",
                "start_date",
                "end_date",
                "alt_names",
            ]
        )
    if processed_cities.empty:
        processed_cities = pd.DataFrame(
            columns=["city_id", "city_name", "state", "country", "slug"]
        )
    if processed_games.empty:
        processed_games = pd.DataFrame(
            columns=[
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
        )

    # Create city entries and team entries
    new_city_rows = []
    new_team_rows = []

    # Prepare lookup sets
    existing_city_ids = set(processed_cities["city_id"].astype(str).tolist())
    existing_team_ids = set(processed_teams["team_id"].astype(str).tolist())

    retro_to_team_id: Dict[str, str] = {}

    for retro in sorted(retro_codes):
        city, state = retro_to_city_state.get(retro, ("", ""))
        # Fallback: leave empty if unknown
        city_name = city or retro
        state_code = state or ""
        city_id = slugify_city(city_name, state_code) if city_name else retro.lower()
        slug = city_id
        if city_id not in existing_city_ids and city_name:
            new_city_rows.append(
                {
                    "city_id": city_id,
                    "city_name": city_name,
                    "state": state_code,
                    "country": "USA",
                    "slug": slug,
                }
            )
            existing_city_ids.add(city_id)

        team_id = f"mlb_{retro}"
        retro_to_team_id[retro] = team_id
        if team_id not in existing_team_ids:
            team_name = f"{city_name} " if city_name else ""
            # Nickname not easily available from CurrentNames reliably; use retro as placeholder
            team_display = f"{team_name}{retro}".strip()
            new_team_rows.append(
                {
                    "team_id": team_id,
                    "team_name": team_display,
                    "league": "mlb",
                    "city_id": city_id,
                    "city_name": city_name,
                    "start_date": "",
                    "end_date": "",
                    "alt_names": retro,
                }
            )
            existing_team_ids.add(team_id)

    if new_city_rows:
        processed_cities = pd.concat([processed_cities, pd.DataFrame(new_city_rows)], ignore_index=True)
    if new_team_rows:
        processed_teams = pd.concat([processed_teams, pd.DataFrame(new_team_rows)], ignore_index=True)

    # Build games rows
    new_game_rows = []
    for _, r in games_csv.iterrows():
        date_raw = str(r["Date"]).strip()
        # Convert yyyymmdd -> yyyy-mm-dd
        date = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}" if len(date_raw) == 8 else date_raw
        vt = str(r["VT"]).strip()
        ht = str(r["HT"]).strip()
        vt_score = int(r["VT Score"]) if pd.notna(r["VT Score"]) else None
        ht_score = int(r["HT Score"]) if pd.notna(r["HT Score"]) else None
        winner_flag = int(r["Game Winner"]) if pd.notna(r["Game Winner"]) else None
        if vt == "" or ht == "":
            continue

        away_team_id = retro_to_team_id.get(vt, f"mlb_{vt}")
        home_team_id = retro_to_team_id.get(ht, f"mlb_{ht}")
        game_id = f"mlb_{date}_{vt}_{ht}"

        if winner_flag is None or vt_score is None or ht_score is None:
            winning_team_id = ""
        else:
            winning_team_id = away_team_id if winner_flag == 1 else home_team_id

        new_game_rows.append(
            {
                "game_id": game_id,
                "date": date,
                "league": "mlb",
                "season_type": "regular",
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_score": ht_score,
                "away_score": vt_score,
                "winning_team_id": winning_team_id,
            }
        )

    if new_game_rows:
        games_df = pd.DataFrame(new_game_rows)
        # Deduplicate by game_id; prefer existing values
        if not processed_games.empty:
            combined = pd.concat([processed_games, games_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["game_id"], keep="first")
        else:
            combined = games_df
        processed_games = combined

    # Persist
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    processed_cities.to_csv(PROCESSED_DIR / "cities.csv", index=False)
    processed_teams.to_csv(PROCESSED_DIR / "teams.csv", index=False)
    processed_games.to_csv(PROCESSED_DIR / "games.csv", index=False)

    print(
        f"Loaded MLB data: +{len(new_city_rows)} cities, +{len(new_team_rows)} teams, +{len(new_game_rows)} games"
    )


def main() -> None:
    load_games_and_entities()


if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

NHL_GAMES_PATH = DATA_DIR / "nhl_season_games_2018_2025.csv"

MULTIWORD_NICKNAMES = {
    "Maple Leafs",
    "Blue Jackets",
    "Golden Knights",
    "Red Wings",
}


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def slugify(text: str) -> str:
    return text.strip().lower().replace(" ", "-").replace(".", "").replace("/", "-")


def split_city_team(full_name: str) -> Tuple[str, str]:
    """Split an NHL team full name into (city, nickname) using heuristics for multiword nicknames.
    Examples:
    - New York Rangers -> (New York, Rangers)
    - Toronto Maple Leafs -> (Toronto, Maple Leafs)
    - Vegas Golden Knights -> (Vegas, Golden Knights)
    - St. Louis Blues -> (St. Louis, Blues)
    - San Jose Sharks -> (San Jose, Sharks)
    """
    name = str(full_name or "").strip()
    if not name:
        return "", ""
    # Try multiword nickname suffix matches
    for nick in MULTIWORD_NICKNAMES:
        if name.endswith(nick):
            city = name[: -len(nick)].strip()
            # Remove trailing space if any leftover
            if city.endswith(" "):
                city = city[:-1]
            return city, nick
    # Fallback: split on last space
    if " " in name:
        parts = name.rsplit(" ", 1)
        return parts[0], parts[1]
    return name, ""


def load_nhl() -> None:
    games_in = safe_read_csv(NHL_GAMES_PATH)
    if games_in.empty:
        print("NHL loader: missing nhl_season_games_2018_2025.csv; skipping.")
        return

    processed_teams = safe_read_csv(PROCESSED_DIR / "teams.csv")
    processed_cities = safe_read_csv(PROCESSED_DIR / "cities.csv")
    processed_games = safe_read_csv(PROCESSED_DIR / "games.csv")

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

    new_city_rows = []
    new_team_rows = []
    new_game_rows = []

    existing_city_ids = set(processed_cities["city_id"].astype(str).tolist())
    existing_team_ids = set(processed_teams["team_id"].astype(str).tolist())

    # Iterate rows
    for _, r in games_in.iterrows():
        date = str(r.get("Date", "")).strip().split(" ")[0]
        away_full = str(r.get("Away", "")).strip()
        home_full = str(r.get("Home", "")).strip()
        away_goals = r.get("AwayGoals")
        home_goals = r.get("HomeGoals")
        game_type = str(r.get("Type", "")).strip()

        # Determine season type
        season_type = "playoff" if "playoff" in game_type.lower() else "regular"

        away_city, away_nick = split_city_team(away_full)
        home_city, home_nick = split_city_team(home_full)

        away_city_id = slugify(away_city)
        home_city_id = slugify(home_city)

        # Ensure cities
        if away_city and away_city_id not in existing_city_ids:
            new_city_rows.append({
                "city_id": away_city_id,
                "city_name": away_city,
                "state": "",
                "country": "USA",
                "slug": away_city_id,
            })
            existing_city_ids.add(away_city_id)
        if home_city and home_city_id not in existing_city_ids:
            new_city_rows.append({
                "city_id": home_city_id,
                "city_name": home_city,
                "state": "",
                "country": "USA",
                "slug": home_city_id,
            })
            existing_city_ids.add(home_city_id)

        away_team_id = f"nhl_{slugify(away_full)}"
        home_team_id = f"nhl_{slugify(home_full)}"

        # Ensure teams
        if away_team_id not in existing_team_ids:
            new_team_rows.append({
                "team_id": away_team_id,
                "team_name": away_full,
                "league": "nhl",
                "city_id": away_city_id,
                "city_name": away_city,
                "start_date": "",
                "end_date": "",
                "alt_names": away_nick,
            })
            existing_team_ids.add(away_team_id)
        if home_team_id not in existing_team_ids:
            new_team_rows.append({
                "team_id": home_team_id,
                "team_name": home_full,
                "league": "nhl",
                "city_id": home_city_id,
                "city_name": home_city,
                "start_date": "",
                "end_date": "",
                "alt_names": home_nick,
            })
            existing_team_ids.add(home_team_id)

        # Parse scores and winner
        try:
            a_goals = int(float(away_goals)) if pd.notna(away_goals) else None
            h_goals = int(float(home_goals)) if pd.notna(home_goals) else None
        except Exception:
            a_goals = None
            h_goals = None
        if a_goals is None or h_goals is None:
            winning_team_id = ""
        else:
            if a_goals > h_goals:
                winning_team_id = away_team_id
            elif h_goals > a_goals:
                winning_team_id = home_team_id
            else:
                winning_team_id = ""  # should not happen in NHL modern rules

        game_id = f"nhl_{date}_{slugify(away_full)}_{slugify(home_full)}"
        new_game_rows.append({
            "game_id": game_id,
            "date": date,
            "league": "nhl",
            "season_type": season_type,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": h_goals if h_goals is not None else "",
            "away_score": a_goals if a_goals is not None else "",
            "winning_team_id": winning_team_id,
        })

    # Append and dedupe
    if new_city_rows:
        processed_cities = pd.concat([processed_cities, pd.DataFrame(new_city_rows)], ignore_index=True)
    if new_team_rows:
        processed_teams = pd.concat([processed_teams, pd.DataFrame(new_team_rows)], ignore_index=True)
    if new_game_rows:
        games_df = pd.DataFrame(new_game_rows)
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

    print(f"Loaded NHL data: +{len(new_city_rows)} cities, +{len(new_team_rows)} teams, +{len(new_game_rows)} games")


def main() -> None:
    load_nhl()


if __name__ == "__main__":
    main()

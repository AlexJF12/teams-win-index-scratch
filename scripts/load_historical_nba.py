from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

NBA_TEAMS_PATH = DATA_DIR / "teams.csv"  # columns: teamId,abbreviation,teamName,simpleName,location
NBA_GAMES_PATH = DATA_DIR / "nba_regular_season_totals_2010_2024_small.csv"


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def slugify(text: str) -> str:
    return text.strip().lower().replace(" ", "-")


def load_nba() -> None:
    teams_in = safe_read_csv(NBA_TEAMS_PATH)
    games_in = safe_read_csv(NBA_GAMES_PATH)

    if teams_in.empty or games_in.empty:
        print("NBA loader: missing teams.csv or nba game CSV; skipping.")
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

    # Seed teams/cities from NBA teams.csv
    new_city_rows = []
    new_team_rows = []
    existing_city_ids = set(processed_cities["city_id"].astype(str).tolist())
    existing_team_ids = set(processed_teams["team_id"].astype(str).tolist())

    abbrev_to_team_id = {}

    for _, t in teams_in.iterrows():
        city_name = str(t["location"]) if "location" in t else ""
        team_name = str(t["teamName"]) if "teamName" in t else ""
        abbrev = str(t["abbreviation"]) if "abbreviation" in t else ""
        team_id_num = str(t["teamId"]) if "teamId" in t else abbrev

        city_id = slugify(city_name)
        if city_id and city_id not in existing_city_ids:
            new_city_rows.append(
                {
                    "city_id": city_id,
                    "city_name": city_name,
                    "state": "",
                    "country": "USA",
                    "slug": city_id,
                }
            )
            existing_city_ids.add(city_id)

        team_id = f"nba_{abbrev}" if abbrev else f"nba_{team_id_num}"
        abbrev_to_team_id[abbrev] = team_id
        if team_id not in existing_team_ids:
            new_team_rows.append(
                {
                    "team_id": team_id,
                    "team_name": f"{city_name} {team_name}".strip(),
                    "league": "nba",
                    "city_id": city_id,
                    "city_name": city_name,
                    "start_date": "",
                    "end_date": "",
                    "alt_names": team_id_num,
                }
            )
            existing_team_ids.add(team_id)

    if new_city_rows:
        processed_cities = pd.concat([processed_cities, pd.DataFrame(new_city_rows)], ignore_index=True)
    if new_team_rows:
        processed_teams = pd.concat([processed_teams, pd.DataFrame(new_team_rows)], ignore_index=True)

    # Build games from per-team W/L rows: need to pair by GAME_ID
    # games_in columns include: TEAM_ABBREVIATION, GAME_ID, GAME_DATE, MATCHUP, WL
    sub = games_in[["TEAM_ABBREVIATION", "GAME_ID", "GAME_DATE", "MATCHUP", "WL"]].copy()
    # Split MATCHUP like "GSW @ POR" or "BKN vs. PHI"
    def parse_matchup(m: str) -> tuple[str, str, bool]:
        m = str(m)
        if "@" in m:
            a, b = m.split("@")
            return a.strip(), b.strip(), True
        if "vs." in m:
            a, b = m.split("vs.")
            return b.strip(), a.strip(), False  # home vs away
        return "", "", False

    parsed = sub.copy()
    parsed[["LEFT", "RIGHT", "is_away_format"]] = parsed["MATCHUP"].apply(
        lambda s: pd.Series(parse_matchup(s))
    )

    # Create a mapping GAME_ID -> home/away abbrev by majority voting
    # For rows with is_away_format True: LEFT is away, RIGHT is home
    # For rows with vs. format: LEFT is home, RIGHT is away (after our parse transformation)
    def infer_home_away(row):
        if row["is_away_format"]:
            return row["LEFT"], row["RIGHT"]  # away, home
        else:
            return row["RIGHT"], row["LEFT"]  # away, home

    tmp = parsed.copy()
    tmp[["away_abbrev", "home_abbrev"]] = tmp.apply(
        lambda r: pd.Series(infer_home_away(r)), axis=1
    )

    # Compute game-level home/away via grouping
    game_side = (
        tmp.groupby("GAME_ID")["home_abbrev"].agg(lambda x: x.mode().iat[0] if not x.mode().empty else x.iloc[0]).to_frame()
        .join(tmp.groupby("GAME_ID")["away_abbrev"].agg(lambda x: x.mode().iat[0] if not x.mode().empty else x.iloc[0]).to_frame())
        .reset_index()
    )

    # Compute winner by checking team rows with WL == 'W'
    winners = (
        sub[sub["WL"] == "W"].groupby("GAME_ID")["TEAM_ABBREVIATION"].agg("first").reset_index().rename(columns={"TEAM_ABBREVIATION": "winner_abbrev"})
    )

    games = (
        game_side.merge(winners, on="GAME_ID", how="left")
        .merge(sub[["GAME_ID", "GAME_DATE"]].drop_duplicates(), on="GAME_ID", how="left")
    )

    # Build processed games rows
    new_games = []
    for _, g in games.iterrows():
        game_id = str(g["GAME_ID"]).strip()
        date = str(g["GAME_DATE"]).split(" ")[0]
        home_abbrev = str(g["home_abbrev"]).strip()
        away_abbrev = str(g["away_abbrev"]).strip()
        winner_abbrev = str(g["winner_abbrev"]).strip() if pd.notna(g["winner_abbrev"]) else ""

        home_team_id = abbrev_to_team_id.get(home_abbrev, f"nba_{home_abbrev}")
        away_team_id = abbrev_to_team_id.get(away_abbrev, f"nba_{away_abbrev}")
        winning_team_id = (
            abbrev_to_team_id.get(winner_abbrev, f"nba_{winner_abbrev}") if winner_abbrev else ""
        )

        new_games.append(
            {
                "game_id": f"nba_{game_id}",
                "date": date,
                "league": "nba",
                "season_type": "regular",
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_score": "",
                "away_score": "",
                "winning_team_id": winning_team_id,
            }
        )

    # Merge into processed_games
    if new_games:
        games_df = pd.DataFrame(new_games)
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

    print(f"Loaded NBA data: +{len(new_city_rows)} cities, +{len(new_team_rows)} teams, +{len(new_games)} games")


def main() -> None:
    load_nba()


if __name__ == "__main__":
    main()

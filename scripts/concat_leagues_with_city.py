from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"

# Inputs (long format minimal schema)
NHL_LONG = OUTPUTS_DIR / "nhl_long.csv"
MLB_LONG = OUTPUTS_DIR / "mlb_long.csv"
NBA_LONG = OUTPUTS_DIR / "nba_long.csv"
NFL_LONG = OUTPUTS_DIR / "nfl_long.csv"

# Auxiliary mappings
NBA_TEAMS_PATH = DATA_DIR / "teams.csv"  # NBA abbrev -> location
MLB_CURRENT_NAMES = DATA_DIR / "CurrentNames.csv"  # retro code -> city

TEAM_CITY_MAP_CSV = DATA_DIR / "team_city_map.csv"
ALL_LONG_WITH_CITY = OUTPUTS_DIR / "all_long.csv"

MULTIWORD_NICKNAMES_NHL = {
    "Maple Leafs",
    "Blue Jackets",
    "Golden Knights",
    "Red Wings",
}

MULTIWORD_NICKNAMES_NFL = {
    "Football Team",  # historical
    "Commanders",
    "49ers",
}


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def split_city_team(full_name: str, multiword_nicknames: set[str]) -> Tuple[str, str]:
    name = str(full_name or "").strip()
    if not name:
        return "", ""
    for nick in multiword_nicknames:
        if name.endswith(nick):
            city = name[: -len(nick)].strip()
            if city.endswith(" "):
                city = city[:-1]
            return city, nick
    if " " in name:
        parts = name.rsplit(" ", 1)
        return parts[0], parts[1]
    return name, ""


def build_nba_abbrev_to_city() -> Dict[str, str]:
    teams = safe_read_csv(NBA_TEAMS_PATH)
    mapping: Dict[str, str] = {}
    if not teams.empty and {"abbreviation", "location"}.issubset(teams.columns):
        for _, r in teams.iterrows():
            abbr = str(r["abbreviation"]).strip()
            city = str(r["location"]).strip()
            if abbr:
                mapping[abbr] = city
    return mapping


def build_mlb_retro_to_city() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not MLB_CURRENT_NAMES.exists():
        return mapping
    with MLB_CURRENT_NAMES.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            retro = row[1].strip() if len(row) > 1 else ""
            city = row[-2].strip() if len(row) >= 2 else ""
            if retro:
                mapping[retro] = city
    return mapping


def make_team_city_map() -> pd.DataFrame:
    frames = []

    # NHL
    nhl = safe_read_csv(NHL_LONG)
    if not nhl.empty:
        teams = (
            nhl[["league", "team"]].drop_duplicates().rename(columns={"team": "team_key"})
        )
        cities = []
        for _, r in teams.iterrows():
            city, _ = split_city_team(r["team_key"], MULTIWORD_NICKNAMES_NHL)
            cities.append(city)
        teams["city"] = cities
        frames.append(teams)

    # MLB (retro codes)
    mlb = safe_read_csv(MLB_LONG)
    if not mlb.empty:
        retro_map = build_mlb_retro_to_city()
        teams = mlb[["league", "team"]].drop_duplicates().rename(columns={"team": "team_key"})
        teams["city"] = teams["team_key"].map(retro_map).fillna(teams["team_key"])  # fallback to code
        frames.append(teams)

    # NBA (abbrev)
    nba = safe_read_csv(NBA_LONG)
    if not nba.empty:
        abbr_map = build_nba_abbrev_to_city()
        teams = nba[["league", "team"]].drop_duplicates().rename(columns={"team": "team_key"})
        teams["city"] = teams["team_key"].map(abbr_map).fillna(teams["team_key"])  # fallback to abbr
        frames.append(teams)

    # NFL (full names)
    nfl = safe_read_csv(NFL_LONG)
    if not nfl.empty:
        teams = nfl[["league", "team"]].drop_duplicates().rename(columns={"team": "team_key"})
        cities = []
        for _, r in teams.iterrows():
            city, _ = split_city_team(r["team_key"], MULTIWORD_NICKNAMES_NFL)
            cities.append(city)
        teams["city"] = cities
        frames.append(teams)

    if not frames:
        return pd.DataFrame(columns=["league", "team", "city"])

    mapping_df = pd.concat(frames, ignore_index=True)
    mapping_df = mapping_df.drop_duplicates(subset=["league", "team_key"]).rename(columns={"team_key": "team"})
    mapping_df = mapping_df[["league", "team", "city"]].sort_values(["league", "team"]).reset_index(drop=True)

    TEAM_CITY_MAP_CSV.parent.mkdir(parents=True, exist_ok=True)
    mapping_df.to_csv(TEAM_CITY_MAP_CSV, index=False)
    return mapping_df


essential_cols = ["date", "team", "index_score", "month", "league"]


def concat_with_city(mapping_df: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    for path in [NHL_LONG, MLB_LONG, NBA_LONG, NFL_LONG]:
        df = safe_read_csv(path)
        if not df.empty:
            df = df[[c for c in essential_cols if c in df.columns]].copy()
            dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=essential_cols + ["city"])

    all_df = pd.concat(dfs, ignore_index=True)
    all_df = all_df.merge(mapping_df, left_on=["league", "team"], right_on=["league", "team"], how="left")
    all_df = all_df.rename(columns={"city": "City"})
    all_df = all_df[essential_cols + ["City"]]

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    all_df.to_csv(ALL_LONG_WITH_CITY, index=False)
    return all_df


def main() -> None:
    mapping_df = make_team_city_map()
    all_df = concat_with_city(mapping_df)
    print(f"Team-city map rows: {len(mapping_df)} → {TEAM_CITY_MAP_CSV}")
    print(f"All long with City rows: {len(all_df)} → {ALL_LONG_WITH_CITY}")


if __name__ == "__main__":
    main()

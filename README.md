# City Sports Happiness Index (Python, CSV, Static Site)

A Python-first project that computes a daily happiness score for each city's sports fans using CSV data and outputs a static site to `docs/`. Daily updates run via GitHub Actions. No backend.

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/update_and_build.py
python -m http.server --directory docs 8000  # http://localhost:8000
```

## Data folders
- `data/processed/` – canonical `cities.csv`, `teams.csv`, `games.csv`, and master `team_game_results.csv`
- `data/daily/` – one CSV per day fetched from APIs
- `data/outputs/` –
  - `city_scores.csv` (history), `city_scores_latest.csv` (snapshot)
  - `team_rollup_weekly.csv`, `team_rollup_monthly.csv`
  - `city_rollup_weekly.csv`, `city_rollup_monthly.csv`

## Master table: `processed/team_game_results.csv`
- Columns: `game_id, date, league, season_type, team_id, opponent_team_id, is_home, team_score, opponent_score, result, index_score, weighted_score, week_start, month_start`
- One row per team per game, enabling flexible time aggregations.

## Config
- `config/scoring.json` – scoring weights
- `config/leagues.json` – leagues included (expand as needed)

## Automation
- `.github/workflows/daily.yml` – scheduled job builds site and commits changes
- Enable GitHub Pages to serve from `docs/` (Settings → Pages)

## Notes
- Historical loaders: `scripts/load_historical_mlb.py`, `scripts/load_historical_nba.py`.
- Daily fetcher is stubbed; integrate API to populate `data/daily/YYYY-MM-DD.csv`. 
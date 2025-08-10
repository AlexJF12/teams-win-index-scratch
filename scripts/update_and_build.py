import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def run(step: str) -> None:
    print(f"\n=== {step} ===")
    result = subprocess.run([sys.executable, str(SCRIPTS_DIR / step)], capture_output=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> None:
    run("seed_cities_and_teams.py")
    run("seed_sample_data.py")
    run("fetch_yesterday.py")
    run("load_historical_mlb.py")
    run("load_historical_nba.py")
    run("load_historical_nhl.py")
    run("compute_team_game_results.py")
    run("calc_city_scores.py")
    run("rollups.py")
    run("generate_site.py")
    run("build_picker_assets.py")


if __name__ == "__main__":
    main() 
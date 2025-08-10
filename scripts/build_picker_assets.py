from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"
DOCS_DIR = REPO_ROOT / "docs"
ASSETS_DIR = DOCS_DIR / "assets"
ALL_LONG = OUTPUTS_DIR / "all_long.csv"


def build_assets() -> tuple[Path, Path]:
    if not ALL_LONG.exists():
        raise FileNotFoundError(f"Missing input: {ALL_LONG}")

    df = pd.read_csv(ALL_LONG)
    required = {"date", "team", "index_score", "month", "league"}
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(f"all_long.csv missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = pd.to_datetime(df["month"], errors="coerce")
    df = df.dropna(subset=["date", "month", "team", "league"]).copy()

    # Teams list per league
    teams = (
        df[["league", "team"]].drop_duplicates().sort_values(["league", "team"]).groupby("league")["team"].apply(list).to_dict()
    )

    # Monthly per league/team from 2022 onward
    df_since = df[(df["date"].dt.year >= 2022)].copy()
    df_since["month_str"] = df_since["month"].dt.strftime("%Y-%m")
    monthly = (
        df_since.groupby(["month_str", "league", "team"], as_index=False)
        .agg(index_sum=("index_score", "sum"), games=("index_score", "count"))
    )

    ASSETS_DIR.mkdir(parents=True, exist_ok=True)

    teams_path = ASSETS_DIR / "teams.json"
    teams_path.write_text(json.dumps(teams, indent=2), encoding="utf-8")

    monthly_path = ASSETS_DIR / "monthly_since_2022.json"
    monthly_records = monthly.to_dict(orient="records")
    monthly_path.write_text(json.dumps(monthly_records), encoding="utf-8")

    return teams_path, monthly_path


def generate_picker_html() -> Path:
    html_path = DOCS_DIR / "picker.html"
    html = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Team Picker – Monthly Index (since 2022)</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }
    .container { max-width: 1100px; margin: 0 auto; }
    label { display: block; font-weight: 600; margin-top: 1rem; }
    select { width: 100%; max-width: 420px; padding: 0.4rem; }
    table { border-collapse: collapse; width: 100%; max-width: 960px; margin-top: 1rem; }
    th, td { border-bottom: 1px solid #eee; padding: 0.5rem; }
    .row { display: grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap: 1rem; }
  </style>
</head>
<body>
<div class="container">
  <p><a href="index.html">← Back</a></p>
  <h1>Pick one team per league – Monthly Index</h1>

  <div class="row">
    <div>
      <label>Year</label>
      <select id="yearSelect"></select>
    </div>
    <div>
      <label>NHL</label>
      <select id="nhlSelect"></select>
    </div>
    <div>
      <label>MLB</label>
      <select id="mlbSelect"></select>
    </div>
    <div>
      <label>NBA</label>
      <select id="nbaSelect"></select>
    </div>
    <div>
      <label>NFL</label>
      <select id="nflSelect"></select>
    </div>
  </div>

  <div id="chart" style="width:100%;max-width:900px;height:420px;margin-top:1rem;"></div>
  <table>
    <thead><tr><th>Month</th><th style="text-align:right">Total Index</th><th style="text-align:right">Games</th></tr></thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>
<script>
async function loadJSON(path) {
  const res = await fetch(path);
  return res.json();
}

function populateSelect(select, items) {
  select.innerHTML = '';
  items.forEach(t => {
    const opt = document.createElement('option');
    opt.value = t;
    opt.textContent = t;
    select.appendChild(opt);
  });
}

function getYears(monthly) {
  const years = new Set();
  monthly.forEach(rec => {
    const y = rec.month_str.split('-')[0];
    years.add(y);
  });
  return Array.from(years).sort();
}

function aggregateMonthly(selected, monthly, year) {
  const months = ["01","02","03","04","05","06","07","08","09","10","11","12"].map(m => `${year}-${m}`);
  const out = months.map(m => ({ month: m, total: 0, games: 0 }));
  const picks = [
    { lg: 'nhl', team: selected.nhl },
    { lg: 'mlb', team: selected.mlb },
    { lg: 'nba', team: selected.nba },
    { lg: 'nfl', team: selected.nfl },
  ];
  for (const rec of monthly) {
    if (!rec.month_str.startsWith(year+"-")) continue;
    for (const p of picks) {
      if (rec.league === p.lg && rec.team === p.team) {
        const idx = months.indexOf(rec.month_str);
        if (idx >= 0) {
          out[idx].total += rec.index_sum;
          out[idx].games += rec.games;
        }
      }
    }
  }
  return out;
}

function render(selected, monthly, year) {
  const agg = aggregateMonthly(selected, monthly, year);
  // Chart
  const x = agg.map(r => r.month);
  const y = agg.map(r => r.total);
  const trace = { x, y, type: 'bar', marker: { color: '#2b8a3e' } };
  Plotly.newPlot('chart', [trace], { title: `Monthly Index (${year})`, margin: {l:40,r:10,t:40,b:40} }, { displayModeBar: false });
  // Table
  const body = document.getElementById('tableBody');
  body.innerHTML = '';
  agg.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.month}</td><td style='text-align:right'>${r.total}</td><td style='text-align:right'>${r.games}</td>`;
    body.appendChild(tr);
  });
}

(async function init() {
  const teams = await loadJSON('assets/teams.json');
  const monthly = await loadJSON('assets/monthly_since_2022.json');
  const years = getYears(monthly);

  const yearSel = document.getElementById('yearSelect');
  populateSelect(yearSel, years);

  const nhlSel = document.getElementById('nhlSelect');
  const mlbSel = document.getElementById('mlbSelect');
  const nbaSel = document.getElementById('nbaSelect');
  const nflSel = document.getElementById('nflSelect');
  populateSelect(nhlSel, (teams['nhl']||[]));
  populateSelect(mlbSel, (teams['mlb']||[]));
  populateSelect(nbaSel, (teams['nba']||[]));
  populateSelect(nflSel, (teams['nfl']||[]));

  const selected = { nhl: nhlSel.value, mlb: mlbSel.value, nba: nbaSel.value, nfl: nflSel.value };
  const year = yearSel.value;
  render(selected, monthly, year);

  [yearSel, nhlSel, mlbSel, nbaSel, nflSel].forEach(sel => sel.addEventListener('change', () => {
    const s = { nhl: nhlSel.value, mlb: mlbSel.value, nba: nbaSel.value, nfl: nflSel.value };
    render(s, monthly, yearSel.value);
  }));
})();
</script>
</body>
</html>
"""
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html, encoding="utf-8")
    return html_path


def main() -> None:
    teams_path, monthly_path = build_assets()
    picker_path = generate_picker_html()
    print(f"Wrote teams → {teams_path}")
    print(f"Wrote monthly_since_2022 → {monthly_path}")
    print(f"Wrote picker page → {picker_path}")


if __name__ == "__main__":
    main()

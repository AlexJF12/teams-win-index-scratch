from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from flask import Flask, jsonify, render_template_string, request

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"
ALL_LONG = OUTPUTS_DIR / "all_long.csv"

app = Flask(__name__)

_df: Optional[pd.DataFrame] = None
_teams_by_league: Optional[Dict[str, List[str]]] = None
_available_years: Optional[List[str]] = None


def _load_data() -> None:
    global _df, _teams_by_league, _available_years
    if _df is not None:
        return
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
    df["league"] = df["league"].str.lower()
    df["month_str"] = df["month"].dt.strftime("%Y-%m")
    df["year_str"] = df["date"].dt.strftime("%Y")

    _df = df
    _teams_by_league = (
        df[["league", "team"]]
        .drop_duplicates()
        .sort_values(["league", "team"])  # type: ignore
        .groupby("league")["team"].apply(list)
        .to_dict()
    )
    _available_years = sorted(df["year_str"].unique().tolist())


TEMPLATE = """
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Team Picker – Monthly Index</title>
  <script src=\"https://cdn.plot.ly/plotly-2.27.0.min.js\"></script>
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
<div class=\"container\">
  <h1>Pick one team per league – Monthly Index</h1>
  <div class=\"row\">
    <div>
      <label>Year</label>
      <select id=\"yearSelect\"></select>
    </div>
    <div>
      <label>NHL</label>
      <select id=\"nhlSelect\"></select>
    </div>
    <div>
      <label>MLB</label>
      <select id=\"mlbSelect\"></select>
    </div>
    <div>
      <label>NBA</label>
      <select id=\"nbaSelect\"></select>
    </div>
    <div>
      <label>NFL</label>
      <select id=\"nflSelect\"></select>
    </div>
  </div>

  <div id=\"chart\" style=\"width:100%;max-width:900px;height:420px;margin-top:1rem;\"></div>
  <table>
    <thead><tr><th>Month</th><th style=\"text-align:right\">Total Index</th><th style=\"text-align:right\">Games</th></tr></thead>
    <tbody id=\"tableBody\"></tbody>
  </table>
</div>
<script>
async function fetchJSON(url) { const r = await fetch(url); return r.json(); }
function populate(select, items) { select.innerHTML=''; items.forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; select.appendChild(o);}); }
function renderChart(months, total) { const trace={x:months,y:total,type:'bar',marker:{color:'#2b8a3e'}}; Plotly.newPlot('chart',[trace],{title:'Monthly Index',margin:{l:40,r:10,t:40,b:40}},{displayModeBar:false}); }
function renderTable(rows) { const t=document.getElementById('tableBody'); t.innerHTML=''; rows.forEach(r=>{ const tr=document.createElement('tr'); tr.innerHTML=`<td>${r.month_str}</td><td style='text-align:right'>${r.total}</td><td style='text-align:right'>${r.games}</td>`; t.appendChild(tr);}); }

(async function init(){
  const meta=await fetchJSON('/api/teams');
  const years=meta.years; const teams=meta.teams;
  const yearSel=document.getElementById('yearSelect');
  const nhlSel=document.getElementById('nhlSelect');
  const mlbSel=document.getElementById('mlbSelect');
  const nbaSel=document.getElementById('nbaSelect');
  const nflSel=document.getElementById('nflSelect');
  populate(yearSel, years);
  populate(nhlSel, teams['nhl']||[]);
  populate(mlbSel, teams['mlb']||[]);
  populate(nbaSel, teams['nba']||[]);
  populate(nflSel, teams['nfl']||[]);

  async function refresh(){
    const y=yearSel.value;
    const url=`/api/monthly?year=${encodeURIComponent(y)}&nhl=${encodeURIComponent(nhlSel.value)}&mlb=${encodeURIComponent(mlbSel.value)}&nba=${encodeURIComponent(nbaSel.value)}&nfl=${encodeURIComponent(nflSel.value)}`;
    const data=await fetchJSON(url);
    renderChart(data.months, data.total);
    renderTable(data.rows);
  }

  await refresh();
  [yearSel, nhlSel, mlbSel, nbaSel, nflSel].forEach(el=> el.addEventListener('change', refresh));
})();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(TEMPLATE)


@app.route("/api/teams")
def api_teams():
    _load_data()
    return jsonify({"teams": _teams_by_league, "years": _available_years})


@app.route("/api/monthly")
def api_monthly():
    _load_data()
    assert _df is not None
    df = _df

    year = request.args.get("year", type=str)
    nhl = request.args.get("nhl", type=str)
    mlb = request.args.get("mlb", type=str)
    nba = request.args.get("nba", type=str)
    nfl = request.args.get("nfl", type=str)
    if not all([year, nhl, mlb, nba, nfl]):
        return jsonify({"error": "Missing required params"}), 400

    picks = {"nhl": nhl, "mlb": mlb, "nba": nba, "nfl": nfl}

    mask = (df["year_str"] == str(year)) & (
        ( (df["league"] == "nhl") & (df["team"] == picks["nhl"]) ) |
        ( (df["league"] == "mlb") & (df["team"] == picks["mlb"]) ) |
        ( (df["league"] == "nba") & (df["team"] == picks["nba"]) ) |
        ( (df["league"] == "nfl") & (df["team"] == picks["nfl"]) )
    )

    sel = df.loc[mask, ["month_str", "index_score"]].copy()
    if sel.empty:
        months = [f"{year}-{m:02d}" for m in range(1, 13)]
        return jsonify({"months": months, "total": [0]*12, "games": [0]*12, "rows": []})

    monthly = (
        sel.groupby("month_str", as_index=False)
        .agg(total=("index_score", "sum"), games=("index_score", "count"))
        .sort_values("month_str")
    )
    months = [f"{year}-{m:02d}" for m in range(1, 13)]
    monthly = monthly.set_index("month_str").reindex(months, fill_value=0).reset_index()

    return jsonify({
        "months": monthly["month_str"].tolist(),
        "total": monthly["total"].tolist(),
        "games": monthly["games"].tolist(),
        "rows": monthly.to_dict(orient="records"),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

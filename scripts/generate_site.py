from __future__ import annotations

import html
import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUTS_DIR = DATA_DIR / "outputs"
PROCESSED_DIR = DATA_DIR / "processed"
DOCS_DIR = REPO_ROOT / "docs"
CITIES_DIR = DOCS_DIR / "cities"


def slugify(text: str) -> str:
    return (
        text.strip().lower().replace(" ", "-").replace("/", "-").replace("&", "and")
    )


def normalize_city_name(name: str) -> str:
    return (name or "").strip().lower()


def read_scores() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    all_path = OUTPUTS_DIR / "city_scores.csv"
    latest_path = OUTPUTS_DIR / "city_scores_latest.csv"
    cities_path = PROCESSED_DIR / "cities.csv"

    all_scores = pd.read_csv(all_path) if all_path.exists() else pd.DataFrame()
    latest = pd.read_csv(latest_path) if latest_path.exists() else pd.DataFrame()
    cities = pd.read_csv(cities_path) if cities_path.exists() else pd.DataFrame()
    return all_scores, latest, cities


def read_rollups() -> tuple[pd.DataFrame, pd.DataFrame]:
    weekly_path = OUTPUTS_DIR / "city_rollup_weekly.csv"
    monthly_path = OUTPUTS_DIR / "city_rollup_monthly.csv"
    weekly = pd.read_csv(weekly_path) if weekly_path.exists() else pd.DataFrame()
    monthly = pd.read_csv(monthly_path) if monthly_path.exists() else pd.DataFrame()
    return weekly, monthly


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_city_groups(cities: pd.DataFrame) -> list[dict]:
    """Return a list of city descriptors: { key, name, slug, city_ids } deduped by normalized name."""
    if cities is None or cities.empty:
        return []
    tmp = cities[["city_id", "city_name"]].dropna()
    tmp["city_key"] = tmp["city_name"].apply(normalize_city_name)
    groups = []
    for key, df in tmp.groupby("city_key"):
        name = str(df.iloc[0]["city_name"])  # display name from first occurrence
        slug = slugify(name)
        city_ids = sorted(df["city_id"].astype(str).unique().tolist())
        groups.append({"key": key, "name": name, "slug": slug, "city_ids": city_ids})
    # Sort alphabetically by name
    groups.sort(key=lambda d: d["name"].lower())
    return groups


def render_index(latest: pd.DataFrame, cities: pd.DataFrame) -> str:
    title = "City Sports Happiness Index"

    city_groups = build_city_groups(cities)

    rows_html = ""
    if latest is not None and not latest.empty and city_groups:
        # Use city_name from latest directly and aggregate by normalized name
        latest2 = latest.copy()
        if "city_name" not in latest2.columns:
            # Fallback: join if needed
            latest2 = latest2.merge(cities[["city_id", "city_name"]], on="city_id", how="left")
        latest2["city_key"] = latest2["city_name"].apply(normalize_city_name)
        agg = (
            latest2.groupby("city_key", as_index=False)
            .agg({
                "score": "sum",
                "wins": "sum",
                "losses": "sum",
                "playoff_wins": "sum",
                "playoff_losses": "sum",
            })
        )
        # Map back to display name and slug
        key_to_meta = {g["key"]: (g["name"], g["slug"]) for g in city_groups}
        agg["display_name"] = agg["city_key"].map(lambda k: key_to_meta.get(k, (k, slugify(k)))[0])
        agg["slug"] = agg["city_key"].map(lambda k: key_to_meta.get(k, (k, slugify(k)))[1])
        latest_sorted = agg.sort_values("score", ascending=False)
        for _, r in latest_sorted.iterrows():
            city = html.escape(str(r.get("display_name", "")))
            score = str(int(r.get("score", 0)))
            slug = r.get("slug", slugify(city))
            rows_html += f"<tr><td><a href=\"cities/{slug}.html\">{city}</a></td><td style=\"text-align:right\">{score}</td></tr>"
    else:
        rows_html = "<tr><td colspan=2>No data yet. Populate teams/cities and games to see scores.</td></tr>"

    cities_list_html = ""
    for g in city_groups:
        cities_list_html += f"<li><a href=\"cities/{g['slug']}.html\">{html.escape(g['name'])}</a></li>"

    return f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 720px; }}
    th, td {{ border-bottom: 1px solid #eee; padding: 0.5rem; }}
    h1, h2 {{ margin: 0.5rem 0; }}
    .container {{ max-width: 960px; margin: 0 auto; }}
  </style>
</head>
<body>
<div class=\"container\">
  <h1>{title}</h1>
  <p>Yesterday's leaderboard</p>
  <table>
    <thead><tr><th>City</th><th style=\"text-align:right\">Score</th></tr></thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>

  <h2>All Cities</h2>
  <ul>
    {cities_list_html}
  </ul>
</div>
</body>
</html>
"""


def chart_script(div_id: str, x: list[str], y: list[float], title: str) -> str:
    data = [{"x": x, "y": y, "type": "bar"}]
    layout = {
        "title": title,
        "margin": {"l": 40, "r": 10, "t": 40, "b": 40},
        "xaxis": {"tickangle": -45},
    }
    return f"""
<div id="{div_id}" style="width:100%;max-width:900px;height:360px;"></div>
<script>
window.addEventListener('DOMContentLoaded', function() {{
  var data = {json.dumps(data)};
  var layout = {json.dumps(layout)};
  if (window.Plotly) {{ Plotly.newPlot('{div_id}', data, layout, {{displayModeBar: false}}); }}
}});
</script>
"""


def render_city_page(city_name: str, city_ids: list[str], all_scores: pd.DataFrame, weekly: pd.DataFrame, monthly: pd.DataFrame) -> str:
    title = f"{city_name} – City Happiness"

    # Aggregate daily across all matching city_ids
    if all_scores is None or all_scores.empty:
        city_scores = pd.DataFrame(columns=["date", "score", "wins", "losses", "playoff_wins", "playoff_losses"])
    else:
        mask = all_scores["city_id"].astype(str).isin(city_ids)
        subset = all_scores[mask].copy()
        if subset.empty:
            city_scores = pd.DataFrame(columns=["date", "score", "wins", "losses", "playoff_wins", "playoff_losses"])
        else:
            city_scores = (
                subset.groupby("date", as_index=False)
                .agg({
                    "score": "sum",
                    "wins": "sum",
                    "losses": "sum",
                    "playoff_wins": "sum",
                    "playoff_losses": "sum",
                })
                .sort_values("date")
            )

    latest = city_scores.iloc[-1] if not city_scores.empty else None
    summary_html = ""
    if latest is not None:
        summary_html = (
            f"<p><strong>Latest date:</strong> {html.escape(str(latest['date']))} | "
            f"<strong>Score:</strong> {int(latest['score'])} | "
            f"W {int(latest['wins'])} / L {int(latest['losses'])} | "
            f"Playoffs W {int(latest['playoff_wins'])} / L {int(latest['playoff_losses'])}</p>"
        )
    else:
        summary_html = "<p>No historical data yet for this city.</p>"

    # Weekly and monthly rollups for plots (aggregate over all matching city_ids)
    weekly_html = ""
    monthly_html = ""

    if weekly is not None and not weekly.empty:
        w = weekly[weekly["city_id"].astype(str).isin(city_ids)].copy()
        if not w.empty:
            w = (
                w.groupby("week_start", as_index=False)
                .agg(weighted_score_sum=("weighted_score_sum", "sum"))
                .sort_values("week_start")
                .tail(52)
            )
            weekly_html = chart_script(
                div_id="weeklyChart",
                x=w["week_start"].astype(str).tolist(),
                y=w["weighted_score_sum"].astype(float).tolist(),
                title="Weekly Weighted Score (last 52 weeks)",
            )

    if monthly is not None and not monthly.empty:
        m = monthly[monthly["city_id"].astype(str).isin(city_ids)].copy()
        if not m.empty:
            m = (
                m.groupby("month_start", as_index=False)
                .agg(weighted_score_sum=("weighted_score_sum", "sum"))
                .sort_values("month_start")
                .tail(24)
            )
            monthly_html = chart_script(
                div_id="monthlyChart",
                x=m["month_start"].astype(str).tolist(),
                y=m["weighted_score_sum"].astype(float).tolist(),
                title="Monthly Weighted Score (last 24 months)",
            )

    # History table
    history_table_html = (
        "<table>\n"
        "<thead><tr><th>Date</th><th style=\"text-align:right\">Score</th>"
        "<th style=\"text-align:right\">W</th><th style=\"text-align:right\">L</th>"
        "<th style=\"text-align:right\">PO W</th><th style=\"text-align:right\">PO L</th></tr></thead>\n"
        "<tbody>"
        + "".join(
            f"<tr><td>{html.escape(str(r['date']))}</td><td style='text-align:right'>{int(r['score'])}</td>"
            f"<td style='text-align:right'>{int(r['wins'])}</td><td style='text-align:right'>{int(r['losses'])}</td>"
            f"<td style='text-align:right'>{int(r['playoff_wins'])}</td><td style='text-align:right'>{int(r['playoff_losses'])}</td></tr>"
            for _, r in city_scores.tail(365).iterrows()
        )
        + "</tbody></table>"
    )

    return f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{html.escape(title)}</title>
  <script src=\"https://cdn.plot.ly/plotly-2.27.0.min.js\"></script>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 960px; }}
    th, td {{ border-bottom: 1px solid #eee; padding: 0.5rem; }}
    h1, h2 {{ margin: 0.5rem 0; }}
    .container {{ max-width: 1100px; margin: 0 auto; }}
    a {{ color: #0a58ca; text-decoration: none; }}
  </style>
</head>
<body>
<div class=\"container\">
  <p><a href=\"../index.html\">← Back</a></p>
  <h1>{html.escape(city_name)}</h1>
  {summary_html}

  <h2>Weekly Trend</h2>
  {weekly_html or '<p>No weekly data.</p>'}

  <h2>Monthly Trend</h2>
  {monthly_html or '<p>No monthly data.</p>'}

  <h2>Daily History (last 365 days)</h2>
  {history_table_html}
</div>
</body>
</html>
"""


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    CITIES_DIR.mkdir(parents=True, exist_ok=True)
    all_scores, latest, cities = read_scores()
    weekly, monthly = read_rollups()

    # One page per unique city name
    city_groups = build_city_groups(cities)

    index_html = render_index(latest, cities)
    write_file(DOCS_DIR / "index.html", index_html)

    for g in city_groups:
        html_content = render_city_page(g["name"], g["city_ids"], all_scores, weekly, monthly)
        write_file(CITIES_DIR / f"{g['slug']}.html", html_content)

    print(f"Site generated in {DOCS_DIR}")


if __name__ == "__main__":
    main() 
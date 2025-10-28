"""Generate a single-page HTML dashboard from 2025 NYC TLC parquet data.

The script queries the public TLC yellow taxi trip records for 2025 and produces a
self-contained HTML dashboard.  The dashboard highlights:

* Circadian pickup patterns (rides per hour of day).
* Night-time drop-off hot spots by hour.
* Ridership trends over the loaded months.

The script streams data directly from the TLC CDN using DuckDB so that we do not
need to download large parquet files locally.  The generated dashboard is stored
under ``outputs/dashboard.html`` by default.
"""

from __future__ import annotations

import argparse
import re
import shutil
import textwrap
from pathlib import Path
from urllib.request import urlopen

import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_MONTHS = ("2025-01", "2025-02", "2025-03")
CACHE_DIR = Path("data/raw")
ZONE_LOOKUP_FILE = CACHE_DIR / "taxi_zone_lookup.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a 2025 NYC Taxi dashboard from the TLC parquet feeds.",
    )
    parser.add_argument(
        "--months",
        nargs="*",
        default=list(DEFAULT_MONTHS),
        help="List of YYYY-MM months to include (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs/dashboard.html"),
        help="Path where the HTML dashboard will be written.",
    )
    return parser.parse_args()


def ensure_local_files(months: list[str]) -> list[Path]:
    """Download each month's parquet file locally if it is not already cached."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    local_paths: list[Path] = []
    for month in months:
        url = f"{TAXI_BASE_URL}/yellow_tripdata_{month}.parquet"
        destination = CACHE_DIR / f"yellow_tripdata_{month}.parquet"
        if not destination.exists():
            print(f"Downloading {url} → {destination} ...")
            with urlopen(url) as response, destination.open("wb") as outfile:
                shutil.copyfileobj(response, outfile)
        local_paths.append(destination.resolve())
    return local_paths


def load_data(months: list[str]) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with remote parquet tables registered."""
    con = duckdb.connect()
    parquet_files = ensure_local_files(months)
    parquet_array = ", ".join(f"'{path.as_posix()}'" for path in parquet_files)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW taxi_trips AS
        SELECT * FROM read_parquet([{parquet_array}])
        WHERE tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL
        """
    )

    if not ZONE_LOOKUP_FILE.exists():
        print(f"Downloading {ZONE_LOOKUP_URL} → {ZONE_LOOKUP_FILE} ...")
        with urlopen(ZONE_LOOKUP_URL) as response, ZONE_LOOKUP_FILE.open("wb") as outfile:
            shutil.copyfileobj(response, outfile)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW taxi_zones AS
        SELECT LocationID, Zone, Borough
        FROM read_csv_auto('{ZONE_LOOKUP_FILE.as_posix()}');
        """
    )
    return con


def compute_hourly_circadian(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Aggregate pickup counts by hour of day to reveal circadian patterns."""
    query = textwrap.dedent(
        """
        SELECT
            EXTRACT('hour' FROM tpep_pickup_datetime) AS pickup_hour,
            COUNT(*)::INTEGER AS trips,
            AVG(passenger_count) AS avg_passenger_count
        FROM taxi_trips
        GROUP BY 1
        ORDER BY 1
        """
    )
    return con.execute(query).df()


def compute_monthly_trend(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Aggregate total trips per day for the covered months."""
    query = textwrap.dedent(
        """
        SELECT
            DATE_TRUNC('day', tpep_pickup_datetime) AS service_day,
            COUNT(*)::INTEGER AS trips
        FROM taxi_trips
        GROUP BY 1
        ORDER BY 1
        """
    )
    return con.execute(query).df()


def compute_night_destinations(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Identify top drop-off destinations during late-night hours (8 PM - 4 AM)."""
    query = textwrap.dedent(
        """
        WITH ranked AS (
            SELECT
                EXTRACT('hour' FROM tpep_pickup_datetime) AS pickup_hour,
                COALESCE(z.Zone, CONCAT('Zone ', CAST(trips.DOLocationID AS VARCHAR))) AS zone_name,
                COUNT(*) AS trips,
                ROW_NUMBER() OVER (
                    PARTITION BY EXTRACT('hour' FROM tpep_pickup_datetime)
                    ORDER BY COUNT(*) DESC
                ) AS zone_rank
            FROM taxi_trips AS trips
            LEFT JOIN taxi_zones AS z ON trips.DOLocationID = z.LocationID
            WHERE (
                EXTRACT('hour' FROM tpep_pickup_datetime) >= 20
                OR EXTRACT('hour' FROM tpep_pickup_datetime) <= 4
            )
            GROUP BY 1, 2
        )
        SELECT pickup_hour, zone_name, trips
        FROM ranked
        WHERE zone_rank <= 8
        ORDER BY pickup_hour, trips DESC;
        """
    )
    return con.execute(query).df()


def build_dashboard(
    hourly: pd.DataFrame, monthly: pd.DataFrame, night: pd.DataFrame, months: list[str]
) -> go.Figure:
    """Compose a one-page dashboard layout using Plotly subplots."""
    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[[{"type": "xy", "colspan": 2}, None], [{"type": "xy"}, {"type": "xy"}]],
        subplot_titles=(
            "Circadian Ridership (Hourly Pickups)",
            "Daily Trip Totals",
            "Nighttime Drop-off Hot Spots",
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    fig.add_trace(
        go.Scatter(
            x=hourly["pickup_hour"],
            y=hourly["trips"],
            mode="lines+markers",
            name="Trips per hour",
            line=dict(color="#1f77b4"),
            hovertemplate="Hour %{x}:00<br>Trips: %{y:,}<br>Avg passengers: %{customdata:.2f}<extra></extra>",
            customdata=hourly["avg_passenger_count"],
        ),
        row=1,
        col=1,
    )
    fig.update_xaxes(title_text="Pickup hour", dtick=1, row=1, col=1)
    fig.update_yaxes(title_text="Trips", row=1, col=1)

    fig.add_trace(
        go.Bar(
            x=monthly["service_day"],
            y=monthly["trips"],
            name="Daily trips",
            marker_color="#ff7f0e",
            hovertemplate="%{x|%b %d}: %{y:,} trips<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.update_xaxes(title_text="Service day", row=2, col=1)
    fig.update_yaxes(title_text="Trips", row=2, col=1)

    if not night.empty:
        # Ensure the bars are sorted so the heatmap is easy to read.
        night_sorted = night.sort_values(["pickup_hour", "trips"], ascending=[True, False])
        heatmap = go.Heatmap(
            x=night_sorted["pickup_hour"],
            y=night_sorted["zone_name"],
            z=night_sorted["trips"],
            colorscale="Viridis",
            colorbar=dict(title="Trips"),
            hovertemplate="Hour %{x}:00<br>%{y}<br>Trips: %{z:,}<extra></extra>",
        )
        fig.add_trace(heatmap, row=2, col=2)
        fig.update_xaxes(title_text="Pickup hour", dtick=1, row=2, col=2)
        fig.update_yaxes(title_text="Drop-off zone", row=2, col=2)
    else:
        fig.add_annotation(
            row=2,
            col=2,
            text="No nighttime trip data available.",
            showarrow=False,
        )

    fig.update_layout(
        title=go.layout.Title(
            text=(
                "NYC Yellow Taxi Ridership Patterns — "
                + ", ".join(months)
            ),
            x=0.0,
            xanchor="left",
        ),
        showlegend=False,
        height=900,
        template="plotly_white",
        margin=dict(l=60, r=40, t=80, b=50),
    )

    return fig


def write_dashboard(fig: go.Figure, output_path: Path, months: list[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    intro = textwrap.dedent(
        f"""
        <h2>Ridership overview</h2>
        <p>
            This dashboard summarizes NYC yellow taxi ridership for {', '.join(months)}.
            Data is sourced from the <a href=\"https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page\">NYC TLC trip record portal</a>
            and queried live from the public parquet files.
        </p>
        """
    )
    html = fig.to_html(full_html=False, include_plotlyjs="cdn")
    document = textwrap.dedent(
        f"""
        <html>
            <head>
                <meta charset=\"utf-8\" />
                <title>NYC Taxi Dashboard</title>
                <style>
                    body {{
                        font-family: 'Helvetica Neue', Arial, sans-serif;
                        margin: 40px auto;
                        max-width: 1200px;
                        line-height: 1.6;
                        color: #111;
                    }}
                    h1 {{
                        margin-bottom: 0;
                    }}
                    h2 {{
                        margin-top: 2em;
                    }}
                    a {{
                        color: #1f77b4;
                    }}
                </style>
            </head>
            <body>
                <h1>NYC Yellow Taxi — Early 2025 Ridership</h1>
                {intro}
                {html}
            </body>
        </html>
        """
    )
    output_path.write_text(document, encoding="utf-8")


def main() -> None:
    args = parse_args()
    months = args.months or list(DEFAULT_MONTHS)
    for value in months:
        if not re.fullmatch(r"\d{4}-\d{2}", value):
            raise ValueError(
                "Months must be provided in YYYY-MM format (e.g. 2025-01)."
            )

    con = load_data(months)
    hourly = compute_hourly_circadian(con)
    monthly = compute_monthly_trend(con)
    night = compute_night_destinations(con)

    figure = build_dashboard(hourly, monthly, night, months)
    write_dashboard(figure, args.output, months)
    print(f"Dashboard written to {args.output.resolve()}")


if __name__ == "__main__":
    main()

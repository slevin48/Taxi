# Taxi

NYC taxi dataset exploration 🚕🚖

## Dashboard generation

This repository now includes a small utility that builds a one-page HTML dashboard
from the NYC Taxi & Limousine Commission (TLC) 2025 yellow taxi trip records. The
script automatically downloads the required public parquet files into `data/raw/`
on first run and reuses the cached copy thereafter.

### Prerequisites

```bash
pip install -r requirements.txt
```

### Generate the dashboard

```bash
python src/dashboard.py
```

By default the script pulls January–March 2025 yellow taxi trips and writes the
rendered dashboard to `outputs/dashboard.html`. Use the `--months` flag to adjust
the range of months (any `YYYY-MM` available on the TLC portal) and `--output` to
customise the destination file.

Open the generated HTML file in your browser to explore:

- Hourly (circadian) ridership patterns.
- Daily trip totals across the selected months.
- The most popular late-night drop-off zones by pickup hour.

#!/usr/bin/env python3
"""
update_tracker.py - Daily Databricks Upload Volume Tracker Generator

Collects daily REST API extraction and Databricks volume upload metrics,
maintains an append-only JSON database (metrics/upload_history.json),
and generates a self-contained, responsive HTML dashboard (upload_tracker.html).
"""

import argparse
import html
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("update_tracker")


def format_bytes(num_bytes: int) -> str:
    """Formats bytes into human-readable B, KB, MB, or GB string."""
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.2f} KB"
    elif num_bytes < 1024 * 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{num_bytes / (1024 * 1024 * 1024):.2f} GB"


def load_history(history_file_path: Path) -> List[Dict[str, Any]]:
    """Loads existing upload history from JSON file or returns empty list."""
    if not history_file_path.exists():
        return []
    try:
        with open(history_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return []
    except Exception as exc:
        logger.warning(f"Could not parse history file {history_file_path}: {exc}")
        return []


def save_history(history_file_path: Path, history: List[Dict[str, Any]]) -> None:
    """Saves updated history list to JSON file with indentation."""
    history_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(history_file_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, default=str)
    logger.info(f"Updated history saved to {history_file_path} ({len(history)} entries)")


def merge_run_summary(
    history: List[Dict[str, Any]],
    summary_data: Dict[str, Any],
    run_id: str,
    trigger_type: str,
) -> List[Dict[str, Any]]:
    """Appends new run summary or updates existing entry if duplicate date/run."""
    entry = {
        "run_id": str(run_id or summary_data.get("run_id") or "local"),
        "timestamp": summary_data.get("timestamp") or datetime.now(timezone.utc).isoformat(),
        "target_date": summary_data.get("target_date", ""),
        "environment": summary_data.get("environment", "dev"),
        "record_count": int(summary_data.get("record_count", 0)),
        "file_size_bytes": int(summary_data.get("file_size_bytes", 0)),
        "file_size_formatted": summary_data.get("file_size_formatted") or format_bytes(int(summary_data.get("file_size_bytes", 0))),
        "volume_target": summary_data.get("volume_target", ""),
        "upload_status": summary_data.get("upload_status", "SUCCESS"),
        "trigger_type": trigger_type or summary_data.get("trigger_type", "manual"),
        "file_name": summary_data.get("file_name", ""),
    }

    # Check for existing entry matching same run_id or same target_date & environment
    matched = False
    for i, item in enumerate(history):
        if (entry["run_id"] != "local" and item.get("run_id") == entry["run_id"]) or (
            item.get("target_date") == entry["target_date"]
            and item.get("environment") == entry["environment"]
            and item.get("file_name") == entry["file_name"]
        ):
            history[i] = entry
            matched = True
            logger.info(f"Updated existing history entry for date {entry['target_date']}")
            break

    if not matched:
        history.append(entry)
        logger.info(f"Appended new history entry for date {entry['target_date']} ({entry['record_count']} records)")

    # Sort history chronologically
    history.sort(key=lambda x: (x.get("target_date", ""), x.get("timestamp", "")))
    return history


def generate_svg_bar_chart(records_data: List[Tuple[str, int]], width: int = 600, height: int = 180) -> str:
    """Generates a responsive SVG bar chart for daily records without external dependencies."""
    if not records_data:
        return "<p class='empty-chart'>No data recorded yet.</p>"

    max_val = max([val for _, val in records_data] + [1])
    padding_x = 45
    padding_y = 25
    chart_w = width - (padding_x * 2)
    chart_h = height - (padding_y * 2)
    num_bars = len(records_data)
    bar_width = max(14, min(40, int(chart_w / (num_bars * 1.5))))

    bars_svg = []
    labels_svg = []

    # Gridlines
    grid_svg = [
        f'<line x1="{padding_x}" y1="{padding_y}" x2="{width - padding_x}" y2="{padding_y}" stroke="#334155" stroke-dasharray="3,3" stroke-width="0.75"/>',
        f'<line x1="{padding_x}" y1="{padding_y + chart_h // 2}" x2="{width - padding_x}" y2="{padding_y + chart_h // 2}" stroke="#334155" stroke-dasharray="3,3" stroke-width="0.75"/>',
        f'<line x1="{padding_x}" y1="{padding_y + chart_h}" x2="{width - padding_x}" y2="{padding_y + chart_h}" stroke="#475569" stroke-width="1"/>',
    ]

    for i, (label, val) in enumerate(records_data):
        x = padding_x + int((i + 0.5) * (chart_w / num_bars)) - (bar_width // 2)
        bar_h = int((val / max_val) * (chart_h - 10)) if max_val > 0 else 0
        y = padding_y + chart_h - bar_h

        # Bar rect with gradient
        bars_svg.append(
            f'<rect x="{x}" y="{y}" width="{bar_width}" height="{bar_h}" rx="4" fill="url(#blueGradient)" opacity="0.9">'
            f'<title>{label}: {val:,} records</title>'
            f'</rect>'
        )
        # Value label on top
        if bar_h > 15:
            bars_svg.append(
                f'<text x="{x + bar_width // 2}" y="{y - 5}" fill="#94a3b8" font-size="10" text-anchor="middle">{val}</text>'
            )

        # X-axis label (MM-DD)
        short_label = label[-5:] if len(label) >= 5 else label
        labels_svg.append(
            f'<text x="{x + bar_width // 2}" y="{padding_y + chart_h + 16}" fill="#64748b" font-size="10" text-anchor="middle">{short_label}</text>'
        )

    svg_content = f"""<svg viewBox="0 0 {width} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="blueGradient" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#38bdf8"/>
      <stop offset="100%" stop-color="#0284c7"/>
    </linearGradient>
  </defs>
  {''.join(grid_svg)}
  {''.join(bars_svg)}
  {''.join(labels_svg)}
</svg>"""
    return svg_content


def generate_svg_area_chart(size_data: List[Tuple[str, float]], width: int = 600, height: int = 180) -> str:
    """Generates a responsive SVG area/line chart for daily data volume (KB)."""
    if not size_data:
        return "<p class='empty-chart'>No data recorded yet.</p>"

    max_val = max([val for _, val in size_data] + [1.0])
    padding_x = 45
    padding_y = 25
    chart_w = width - (padding_x * 2)
    chart_h = height - (padding_y * 2)
    num_pts = len(size_data)

    points = []
    markers = []
    labels_svg = []

    for i, (label, val) in enumerate(size_data):
        x = padding_x + int(i * (chart_w / max(1, num_pts - 1))) if num_pts > 1 else padding_x + (chart_w // 2)
        y = padding_y + chart_h - int((val / max_val) * (chart_h - 10))
        points.append((x, y, label, val))

    # Construct line & area paths
    if points:
        line_path = "M " + " L ".join([f"{x},{y}" for x, y, _, _ in points])
        area_path = (
            f"M {points[0][0]},{padding_y + chart_h} "
            + " L ".join([f"{x},{y}" for x, y, _, _ in points])
            + f" L {points[-1][0]},{padding_y + chart_h} Z"
        )
    else:
        line_path = ""
        area_path = ""

    for x, y, label, val in points:
        markers.append(
            f'<circle cx="{x}" cy="{y}" r="4" fill="#34d399" stroke="#0f172a" stroke-width="2">'
            f'<title>{label}: {val:.2f} KB</title>'
            f'</circle>'
        )
        markers.append(
            f'<text x="{x}" y="{y - 7}" fill="#94a3b8" font-size="10" text-anchor="middle">{val:.1f}k</text>'
        )
        short_label = label[-5:] if len(label) >= 5 else label
        labels_svg.append(
            f'<text x="{x}" y="{padding_y + chart_h + 16}" fill="#64748b" font-size="10" text-anchor="middle">{short_label}</text>'
        )

    grid_svg = [
        f'<line x1="{padding_x}" y1="{padding_y}" x2="{width - padding_x}" y2="{padding_y}" stroke="#334155" stroke-dasharray="3,3" stroke-width="0.75"/>',
        f'<line x1="{padding_x}" y1="{padding_y + chart_h // 2}" x2="{width - padding_x}" y2="{padding_y + chart_h // 2}" stroke="#334155" stroke-dasharray="3,3" stroke-width="0.75"/>',
        f'<line x1="{padding_x}" y1="{padding_y + chart_h}" x2="{width - padding_x}" y2="{padding_y + chart_h}" stroke="#475569" stroke-width="1"/>',
    ]

    svg_content = f"""<svg viewBox="0 0 {width} {height}" class="chart-svg" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="emeraldArea" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#34d399" stop-opacity="0.35"/>
      <stop offset="100%" stop-color="#34d399" stop-opacity="0.0"/>
    </linearGradient>
  </defs>
  {''.join(grid_svg)}
  <path d="{area_path}" fill="url(#emeraldArea)"/>
  <path d="{line_path}" fill="none" stroke="#34d399" stroke-width="2.5" stroke-linecap="round"/>
  {''.join(markers)}
  {''.join(labels_svg)}
</svg>"""
    return svg_content


def render_html_dashboard(history: List[Dict[str, Any]], generated_at: datetime) -> str:
    """Renders the comprehensive, standalone HTML dashboard."""
    total_runs = len(history)
    total_records = sum(int(item.get("record_count", 0)) for item in history)
    total_bytes = sum(int(item.get("file_size_bytes", 0)) for item in history)
    successful_runs = sum(1 for item in history if item.get("upload_status") in ("SUCCESS", "STAGED_LOCAL"))
    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 100.0

    latest_entry = history[-1] if history else {}
    latest_date = latest_entry.get("target_date", "N/A")
    latest_status = latest_entry.get("upload_status", "N/A")
    latest_env = (latest_entry.get("environment") or "dev").upper()
    latest_volume = latest_entry.get("volume_target", "bronze_prod/ecommerce/raw_landing/api_orders")

    # Chart datasets (last 14 runs)
    recent_runs = history[-14:]
    records_chart_data = [(r.get("target_date", ""), int(r.get("record_count", 0))) for r in recent_runs]
    size_chart_data = [(r.get("target_date", ""), round(int(r.get("file_size_bytes", 0)) / 1024, 2)) for r in recent_runs]

    svg_bar = generate_svg_bar_chart(records_chart_data)
    svg_area = generate_svg_area_chart(size_chart_data)

    # Build Table Rows (reverse chronological order)
    table_rows = []
    for item in reversed(history):
        status = item.get("upload_status", "SUCCESS")
        status_class = "badge-success" if status == "SUCCESS" else ("badge-warning" if status == "STAGED_LOCAL" else "badge-danger")
        env = (item.get("environment") or "dev").upper()
        env_class = "badge-prod" if env == "PROD" else "badge-dev"
        date_str = item.get("target_date", "")
        records = f"{int(item.get('record_count', 0)):,}"
        size_fmt = item.get("file_size_formatted") or format_bytes(int(item.get("file_size_bytes", 0)))
        vol = item.get("volume_target", "")
        trigger = item.get("trigger_type", "schedule")
        run_id = item.get("run_id", "")
        ts = item.get("timestamp", "")
        file_name = item.get("file_name", "")

        row_html = f"""<tr>
  <td class="font-mono text-cyan">{html.escape(date_str)}</td>
  <td><span class="badge {env_class}">{html.escape(env)}</span></td>
  <td class="font-bold text-white">{records}</td>
  <td>{html.escape(size_fmt)}</td>
  <td class="font-mono text-xs text-muted" title="{html.escape(vol)}">{html.escape(vol.split('/')[-1] if vol else 'api_orders')}</td>
  <td><span class="badge {status_class}">{html.escape(status)}</span></td>
  <td class="text-xs text-muted">{html.escape(trigger)}</td>
  <td class="text-xs text-muted font-mono">{html.escape(run_id[:8] if run_id else 'local')}</td>
  <td class="text-xs text-muted">{html.escape(ts[:16].replace('T', ' '))}</td>
</tr>"""
        table_rows.append(row_html)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Databricks Lakehouse: Daily Upload Volume Tracker</title>
  <style>
    :root {{
      --bg: #0b0f19;
      --card-bg: #111827;
      --card-border: #1f2937;
      --text-main: #f3f4f6;
      --text-muted: #94a3b8;
      --primary: #38bdf8;
      --emerald: #34d399;
      --amber: #fbbf24;
      --rose: #f43f5e;
      --cyan: #22d3ee;
      --font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background-color: var(--bg);
      color: var(--text-main);
      font-family: var(--font-family);
      line-height: 1.5;
      padding: 24px;
      min-height: 100vh;
    }}
    .container {{
      max-width: 1200px;
      margin: 0 auto;
    }}
    /* Header */
    header {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 24px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 28px;
    }}
    .header-title h1 {{
      font-size: 24px;
      font-weight: 700;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .header-title p {{
      color: var(--text-muted);
      font-size: 14px;
      margin-top: 4px;
    }}
    .live-indicator {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: rgba(52, 211, 153, 0.1);
      border: 1px solid rgba(52, 211, 153, 0.25);
      color: var(--emerald);
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 600;
    }}
    .live-dot {{
      width: 8px;
      height: 8px;
      background: var(--emerald);
      border-radius: 50%;
      box-shadow: 0 0 8px var(--emerald);
    }}
    /* KPI Cards */
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }}
    .kpi-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
      position: relative;
      overflow: hidden;
    }}
    .kpi-card::before {{
      content: "";
      position: absolute;
      top: 0; left: 0; right: 0; height: 3px;
      background: linear-gradient(90deg, var(--primary), var(--cyan));
    }}
    .kpi-card.emerald::before {{ background: linear-gradient(90deg, var(--emerald), #10b981); }}
    .kpi-card.amber::before {{ background: linear-gradient(90deg, var(--amber), #f59e0b); }}
    .kpi-card.purple::before {{ background: linear-gradient(90deg, #c084fc, #a855f7); }}

    .kpi-label {{
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
    }}
    .kpi-value {{
      font-size: 28px;
      font-weight: 700;
      color: #fff;
      margin-top: 8px;
    }}
    .kpi-subtext {{
      font-size: 13px;
      color: var(--text-muted);
      margin-top: 4px;
    }}
    /* Charts Grid */
    .charts-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
      gap: 20px;
      margin-bottom: 28px;
    }}
    .chart-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
    }}
    .chart-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;
    }}
    .chart-header h3 {{
      font-size: 15px;
      font-weight: 600;
      color: #fff;
    }}
    .chart-svg {{
      width: 100%;
      height: 180px;
      display: block;
    }}
    /* Table Section */
    .table-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 24px;
    }}
    .table-header {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 16px;
    }}
    .table-header h3 {{
      font-size: 16px;
      font-weight: 600;
      color: #fff;
    }}
    .search-input {{
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 8px 12px;
      color: #fff;
      font-size: 13px;
      width: 260px;
      outline: none;
      transition: border-color 0.2s;
    }}
    .search-input:focus {{
      border-color: var(--primary);
    }}
    .table-responsive {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      text-align: left;
    }}
    th {{
      background: #0f172a;
      color: var(--text-muted);
      padding: 10px 14px;
      font-weight: 600;
      border-bottom: 1px solid var(--card-border);
    }}
    td {{
      padding: 12px 14px;
      border-bottom: 1px solid #1a2234;
    }}
    tr:hover td {{
      background: rgba(255, 255, 255, 0.02);
    }}
    /* Badges */
    .badge {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 600;
    }}
    .badge-success {{ background: rgba(52, 211, 153, 0.15); color: var(--emerald); border: 1px solid rgba(52, 211, 153, 0.3); }}
    .badge-warning {{ background: rgba(251, 191, 36, 0.15); color: var(--amber); border: 1px solid rgba(251, 191, 36, 0.3); }}
    .badge-danger  {{ background: rgba(244, 63, 94, 0.15); color: var(--rose); border: 1px solid rgba(244, 63, 94, 0.3); }}
    .badge-prod    {{ background: rgba(192, 132, 252, 0.15); color: #c084fc; border: 1px solid rgba(192, 132, 252, 0.3); }}
    .badge-dev     {{ background: rgba(56, 189, 248, 0.15); color: var(--primary); border: 1px solid rgba(56, 189, 248, 0.3); }}

    .font-mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }}
    .font-bold {{ font-weight: 700; }}
    .text-white {{ color: #fff; }}
    .text-cyan {{ color: var(--cyan); }}
    .text-muted {{ color: var(--text-muted); }}
    .text-xs {{ font-size: 11px; }}

    footer {{
      text-align: center;
      padding-top: 16px;
      color: #64748b;
      font-size: 12px;
      border-top: 1px solid var(--card-border);
    }}
    @media (max-width: 640px) {{
      .charts-grid {{ grid-template-columns: 1fr; }}
      .search-input {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="header-title">
        <h1>
          <span>⚡ Databricks Lakehouse</span>
          <span style="color: var(--text-muted); font-weight: 400;">|</span>
          <span style="color: var(--primary);">Upload Volume Tracker</span>
        </h1>
        <p>Continuous daily ingestion tracking into Unity Catalog Landing Volumes (Auto Loader)</p>
      </div>
      <div>
        <span class="live-indicator">
          <span class="live-dot"></span>
          Automated Daily Pipeline Active
        </span>
      </div>
    </header>

    <!-- KPI Summary Cards -->
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Records Ingested</div>
        <div class="kpi-value">{total_records:,}</div>
        <div class="kpi-subtext">Across {total_runs} daily scheduled runs</div>
      </div>
      <div class="kpi-card emerald">
        <div class="kpi-label">Total Data Volume</div>
        <div class="kpi-value">{format_bytes(total_bytes)}</div>
        <div class="kpi-subtext">{total_bytes:,} total bytes uploaded</div>
      </div>
      <div class="kpi-card amber">
        <div class="kpi-label">Upload Success Rate</div>
        <div class="kpi-value">{success_rate:.1f}%</div>
        <div class="kpi-subtext">{successful_runs} of {total_runs} uploads verified</div>
      </div>
      <div class="kpi-card purple">
        <div class="kpi-label">Latest Active Ingestion</div>
        <div class="kpi-value" style="font-size: 22px;">{html.escape(latest_date)}</div>
        <div class="kpi-subtext"><span class="badge badge-{latest_env.lower()}">{latest_env}</span> &bull; {html.escape(latest_status)}</div>
      </div>
    </div>

    <!-- Interactive SVG Vector Trend Charts -->
    <div class="charts-grid">
      <div class="chart-card">
        <div class="chart-header">
          <h3>Daily Ingested Records (Last 14 Batches)</h3>
          <span class="text-xs text-muted">Records / Day</span>
        </div>
        {svg_bar}
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Daily Payload Volume (Last 14 Batches)</h3>
          <span class="text-xs text-muted">Payload (KB)</span>
        </div>
        {svg_area}
      </div>
    </div>

    <!-- Detailed Historical Log Table -->
    <div class="table-card">
      <div class="table-header">
        <h3>Historical Databricks Volume Upload Log ({total_runs} Batches)</h3>
        <input type="text" id="tableFilter" class="search-input" placeholder="Search by date, env, status..." onkeyup="filterTable()">
      </div>
      <div class="table-responsive">
        <table id="uploadsTable">
          <thead>
            <tr>
              <th>Target Date</th>
              <th>Env</th>
              <th>Records</th>
              <th>Payload Size</th>
              <th>Landing Volume</th>
              <th>Upload Status</th>
              <th>Trigger</th>
              <th>Run ID</th>
              <th>Logged (UTC)</th>
            </tr>
          </thead>
          <tbody>
            {''.join(table_rows)}
          </tbody>
        </table>
      </div>
    </div>

    <footer>
      Updated automatically by GitHub Actions CI/CD on {generated_at.strftime('%Y-%m-%d %H:%M:%S UTC')} &bull; Databricks Medallion Architecture
    </footer>
  </div>

  <script>
    function filterTable() {{
      const query = document.getElementById("tableFilter").value.toLowerCase();
      const rows = document.querySelectorAll("#uploadsTable tbody tr");
      rows.forEach(row => {{
        const text = row.innerText.toLowerCase();
        row.style.display = text.includes(query) ? "" : "none";
      }});
    }}
  </script>
</body>
</html>
"""
    return html_content


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Update daily Databricks upload tracker and regenerate HTML dashboard.")
    parser.add_argument("--summary-file", type=str, default="metrics/latest_run_summary.json", help="Path to latest run summary JSON.")
    parser.add_argument("--history-file", type=str, default="metrics/upload_history.json", help="Path to historical uploads JSON database.")
    parser.add_argument("--output-html", type=str, default="upload_tracker.html", help="Path to output HTML dashboard.")
    parser.add_argument("--run-id", type=str, default="", help="GitHub Action run ID or execution ID.")
    parser.add_argument("--trigger-type", type=str, default="", help="Trigger type (schedule | workflow_dispatch | manual).")
    args = parser.parse_args(argv)

    history_file = Path(args.history_file)
    summary_file = Path(args.summary_file)
    output_html_file = Path(args.output_html)

    # 1. Load history
    history = load_history(history_file)

    # 2. Ingest latest run summary if available
    if summary_file.exists():
        try:
            with open(summary_file, "r", encoding="utf-8") as f:
                summary_data = json.load(f)
            history = merge_run_summary(history, summary_data, args.run_id, args.trigger_type)
            save_history(history_file, history)
        except Exception as exc:
            logger.warning(f"Error reading summary file {summary_file}: {exc}")
    else:
        logger.info(f"Summary file {summary_file} not found; generating HTML from existing history ({len(history)} records).")

    # 3. Generate HTML Dashboard
    now_utc = datetime.now(timezone.utc)
    html_dashboard = render_html_dashboard(history, now_utc)

    output_html_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_html_file, "w", encoding="utf-8") as f:
        f.write(html_dashboard)

    # Mirror to index.html for direct GitHub Pages root URL serving
    index_file = output_html_file.parent / "index.html"
    with open(index_file, "w", encoding="utf-8") as f:
        f.write(html_dashboard)

    logger.info(f"Successfully generated upload volume tracker dashboard: {output_html_file} and {index_file}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

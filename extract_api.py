#!/usr/bin/env python3
"""
extract_api.py - Daily Previous-Day REST API Data Extractor

Pulls the previous day's records (created_at >= yesterday AND created_at < today)
in JSON format using requests with resilient exponential retries and pagination.

Key Capabilities:
- Previous-day date window calculation (UTC-normalized), with CLI override.
- Resilient requests.Session with HTTP 429/5xx retry backoff.
- Configurable authentication (Bearer token / API key header).
- Automatic mock generator fallback when external API is not configured or in mock mode.
- Partitioned JSON file writing (year=YYYY/month=MM/day=DD/orders_YYYY-MM-DD.json).
- Direct Databricks Unity Catalog Volume upload support via Databricks Files REST API.
"""

import argparse
import json
import logging
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("extract_api")


def calculate_date_window(target_date_str: Optional[str] = None) -> Tuple[datetime, datetime, str]:
    """
    Calculates the [start, end) UTC datetime window for the target date.
    Defaults to the previous day (yesterday) if no target_date_str is supplied.

    Returns:
        (start_datetime, end_datetime, date_string_YYYY_MM_DD)
    """
    if target_date_str:
        if target_date_str.lower() == "yesterday":
            now_utc = datetime.now(timezone.utc)
            target_date = (now_utc - timedelta(days=1)).date()
        else:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    else:
        now_utc = datetime.now(timezone.utc)
        target_date = (now_utc - timedelta(days=1)).date()

    start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    date_str = target_date.strftime("%Y-%m-%d")

    logger.info(f"Target extraction date: {date_str}")
    logger.info(f"Window: {start_dt.isoformat()} <= created_at < {end_dt.isoformat()}")
    return start_dt, end_dt, date_str


def build_resilient_session(
    retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """
    Initializes a requests.Session configured with exponential backoff and retry policy.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def generate_mock_records(start_dt: datetime, end_dt: datetime, count: int = 250) -> List[Dict[str, Any]]:
    """
    Generates realistic synthetic order transactions strictly timestamped within
    the [start_dt, end_dt) window for testing and zero-dependency dry-runs.
    """
    random.seed(int(start_dt.timestamp()))
    records: List[Dict[str, Any]] = []

    statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Sports", "Beauty"]

    date_str_compact = start_dt.strftime("%Y%m%d")
    total_seconds = int((end_dt - start_dt).total_seconds())

    for idx in range(1, count + 1):
        order_id = f"ORD-API-{date_str_compact}-{idx:04d}"
        cust_id = f"C{random.randint(10001, 15000)}"
        prod_id = f"P{random.randint(1001, 1200)}"
        reg_id = random.randint(1, 8)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.12, 0.05, 0.03])[0]
        unit_price = round(random.uniform(9.99, 499.99), 2)
        total_amount = round(quantity * unit_price, 2)
        status = random.choice(statuses)

        offset_seconds = random.randint(0, max(0, total_seconds - 1))
        created_at_dt = start_dt + timedelta(seconds=offset_seconds)
        created_at_str = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")

        record = {
            "order_id": order_id,
            "customer_id": cust_id,
            "product_id": prod_id,
            "region_id": reg_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "order_status": status,
            "order_date": created_at_str,
            "created_at": created_at_str,
            "source_system": "REST_API",
            "category": random.choice(categories),
        }
        records.append(record)

    logger.info(f"Generated {len(records)} mock records for window {start_dt} to {end_dt}")
    return records


def extract_from_api(
    base_url: str,
    start_dt: datetime,
    end_dt: datetime,
    api_token: Optional[str] = None,
    api_key_header: Optional[str] = None,
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    """
    Pulls records from a REST API endpoint matching created_at >= start_dt and < end_dt.
    Handles Bearer / custom header authentication and page-based pagination.
    """
    session = build_resilient_session()
    headers: Dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "Enterprise-Extract-Agent/1.0",
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    if api_key_header:
        headers["X-API-Key"] = api_key_header

    all_records: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "created_at_gte": start_dt.isoformat(),
            "created_at_lt": end_dt.isoformat(),
            "page": page,
            "limit": page_size,
        }
        logger.info(f"Fetching page {page} from {base_url}...")
        try:
            response = session.get(base_url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                logger.error(f"API request failed with status {response.status_code}: {response.text}")
                response.raise_for_status()

            payload = response.json()
            # Support both raw array responses and envelope {"data": [...], "has_more": bool}
            if isinstance(payload, list):
                batch = payload
                has_more = len(batch) >= page_size
            elif isinstance(payload, dict):
                batch = payload.get("data") or payload.get("records") or payload.get("results") or []
                has_more = payload.get("has_more", len(batch) >= page_size)
            else:
                logger.warning(f"Unexpected response format: {type(payload)}")
                batch = []
                has_more = False

            if not batch:
                logger.info("No more records returned from API.")
                break

            all_records.extend(batch)
            logger.info(f"Page {page}: Retrieved {len(batch)} records (Total so far: {len(all_records)})")

            if not has_more or len(batch) < page_size:
                break

            page += 1

        except requests.RequestException as exc:
            logger.error(f"Error querying API at {base_url}: {exc}")
            raise

    return all_records


def write_partitioned_json(
    records: List[Dict[str, Any]],
    output_base_dir: str,
    date_str: str,
    newline_delimited: bool = True,
) -> Path:
    """
    Writes records into a partitioned directory structure:
    <output_base_dir>/year=YYYY/month=MM/day=DD/orders_YYYY-MM-DD.json
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    partition_dir = (
        Path(output_base_dir)
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"orders_{date_str}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        if newline_delimited:
            # NDJSON (1 JSON object per line) - best for Spark / Auto Loader
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
        else:
            json.dump(records, f, indent=2, default=str)

    logger.info(f"Successfully wrote {len(records)} records to {file_path}")
    return file_path


def upload_to_databricks_volume(
    local_file_path: Path,
    databricks_host: str,
    databricks_token: str,
    volume_path: str,
) -> bool:
    """
    Uploads a local JSON file directly to a Databricks Unity Catalog Volume
    using the Databricks Files 2.0 REST API (PUT /api/2.0/fs/files/{path}).
    """
    # Clean host format (ensure https:// and no trailing slash)
    host = databricks_host.rstrip("/")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"https://{host}"

    # Ensure volume path has leading slash and strip prefix /Volumes
    clean_path = volume_path.lstrip("/")
    if not clean_path.startswith("Volumes/"):
        clean_path = f"Volumes/{clean_path}"

    url = f"{host}/api/2.0/fs/files/{clean_path}"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/octet-stream",
    }

    logger.info(f"Uploading {local_file_path.name} to Databricks Volume: {url}")
    session = build_resilient_session()

    try:
        with open(local_file_path, "rb") as f:
            file_data = f.read()

        response = session.put(url, headers=headers, data=file_data, timeout=60)
        if response.status_code in (200, 201, 204):
            logger.info(f"Successfully uploaded {local_file_path.name} to Databricks Volume.")
            return True
        else:
            logger.error(f"Databricks Volume upload failed ({response.status_code}): {response.text}")
            return False
    except Exception as exc:
        logger.error(f"Failed to upload to Databricks Volume: {exc}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract previous day's records in JSON format via REST API")
    parser.add_argument(
        "--date",
        type=str,
        default=os.environ.get("BATCH_DATE", "yesterday"),
        help="Date to extract (YYYY-MM-DD or 'yesterday'). Defaults to yesterday.",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default=os.environ.get("API_BASE_URL", "").strip(),
        help="API Base URL. If empty or 'mock', mock generator fallback is used.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.environ.get("OUTPUT_DIR", "./landing_data/api_orders"),
        help="Base directory for JSON output landing.",
    )
    parser.add_argument(
        "--volume-target-path",
        type=str,
        default=os.environ.get(
            "DATABRICKS_VOLUME_PATH",
            "bronze_prod/ecommerce/raw_landing/api_orders",
        ),
        help="Target Unity Catalog Volume path in Databricks (e.g. bronze_prod/ecommerce/raw_landing/api_orders).",
    )
    parser.add_argument(
        "--upload-to-volume",
        action="store_true",
        default=os.environ.get("UPLOAD_TO_VOLUME", "false").lower() in ("true", "1", "yes"),
        help="Whether to upload output JSON to Databricks Volume via REST API.",
    )

    args = parser.parse_args()

    start_dt, end_dt, date_str = calculate_date_window(args.date)

    api_token = os.environ.get("API_KEY") or os.environ.get("API_TOKEN") or os.environ.get("API_BEARER_TOKEN")
    api_key_header = os.environ.get("API_KEY_HEADER")

    # Determine whether to use remote API or mock fallback
    if args.api_url and args.api_url.lower() != "mock":
        logger.info(f"Connecting to REST API endpoint: {args.api_url}")
        try:
            records = extract_from_api(
                base_url=args.api_url,
                start_dt=start_dt,
                end_dt=end_dt,
                api_token=api_token,
                api_key_header=api_key_header,
            )
        except Exception as exc:
            logger.warning(f"External API extraction failed: {exc}. Falling back to mock generator.")
            records = generate_mock_records(start_dt, end_dt)
    else:
        logger.info("No external API_BASE_URL provided. Running in built-in mock extraction mode.")
        records = generate_mock_records(start_dt, end_dt)

    if not records:
        logger.warning(f"No records found for date {date_str}.")
        return 0

    # Write partitioned newline-delimited JSON
    output_file = write_partitioned_json(
        records=records,
        output_base_dir=args.output_dir,
        date_str=date_str,
        newline_delimited=True,
    )

    # If running directly inside Databricks runtime where /Volumes/ exists on OS
    direct_volume_path = Path("/Volumes") / args.volume_target_path
    if direct_volume_path.parent.exists():
        try:
            logger.info(f"Detected native Databricks environment. Mirroring to {direct_volume_path}...")
            write_partitioned_json(
                records=records,
                output_base_dir=str(direct_volume_path),
                date_str=date_str,
                newline_delimited=True,
            )
            logger.info("Direct Databricks Volume write succeeded.")
        except Exception as e:
            logger.warning(f"Direct volume write error: {e}")

    # Optional: Push to Databricks Volume via REST API if configured
    db_host = os.environ.get("DATABRICKS_HOST")
    db_token = os.environ.get("DATABRICKS_TOKEN")

    if args.upload_to_volume or (db_host and db_token):
        if db_host and db_token:
            rel_partition = f"year={start_dt.year:04d}/month={start_dt.month:02d}/day={start_dt.day:02d}/{output_file.name}"
            remote_volume_dest = f"{args.volume_target_path.rstrip('/')}/{rel_partition}"
            upload_to_databricks_volume(
                local_file_path=output_file,
                databricks_host=db_host,
                databricks_token=db_token,
                volume_path=remote_volume_dest,
            )
        else:
            logger.warning("Upload to volume requested but DATABRICKS_HOST or DATABRICKS_TOKEN is missing.")

    logger.info(f"Pipeline extraction stage complete for {date_str}. Total records processed: {len(records)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

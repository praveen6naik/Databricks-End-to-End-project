#!/usr/bin/env python3
"""
extract_api.py - Daily Previous-Day REST API Data Extractor (Production Grade)

Pulls or synthesizes the previous day's e-commerce records (created_at >= yesterday AND created_at < today)
with rich, realistic attributes (multi-item cart arrays, payments, shipping, discounts, taxes),
day-of-week seasonality, intraday traffic curves, order lifecycle CDC transitions, and live public API integration.

Key Capabilities:
- Previous-day date window calculation (UTC-normalized), with CLI override.
- Resilient requests.Session with HTTP 429/5xx retry backoff and custom User-Agent.
- Dynamic daily volume modeling factoring in day-of-week seasonality (e.g., weekend surges) and organic variance.
- Intraday peak hour distribution (lunch 11:00-14:00 UTC and evening 18:00-22:00 UTC traffic peaks).
- Deep, realistic e-commerce schema: multi-item carts, payment gateways, shipping carriers, tracking, promo codes, regional taxes.
- Order lifecycle CDC updates: status transitions for historical orders testing Delta Lake MERGE.
- Live public API connectors: Frankfurter ECB currency exchange rates and DummyJSON live carts.
- Automatic mock generator fallback when external API is unreachable or in mock mode.
- Partitioned JSON file writing (year=YYYY/month=MM/day=DD/orders_YYYY-MM-DD.json).
- Direct Databricks Unity Catalog Volume upload support via Databricks Files REST API.
"""

import argparse
import json
import logging
import math
import os
import random
import sys
import uuid
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

# ==============================================================================
# Master Reference Data for Realistic Enterprise E-Commerce
# ==============================================================================

REGIONS_MASTER = {
    1: {
        "name": "North America",
        "country_code": "US",
        "currency": "USD",
        "tax_rate": 0.0825,
        "cities": [
            ("New York", "NY", "10001"),
            ("Los Angeles", "CA", "90001"),
            ("Chicago", "IL", "60601"),
            ("Seattle", "WA", "98101"),
            ("Austin", "TX", "78701"),
        ],
    },
    2: {
        "name": "Western Europe",
        "country_code": "DE",
        "currency": "EUR",
        "tax_rate": 0.1900,
        "cities": [
            ("Berlin", "BE", "10115"),
            ("Munich", "BY", "80331"),
            ("Frankfurt", "HE", "60311"),
            ("Hamburg", "HH", "20095"),
        ],
    },
    3: {
        "name": "United Kingdom",
        "country_code": "GB",
        "currency": "GBP",
        "tax_rate": 0.2000,
        "cities": [
            ("London", "ENG", "EC1A 1BB"),
            ("Manchester", "ENG", "M1 1AE"),
            ("Edinburgh", "SCT", "EH1 1YZ"),
            ("Bristol", "ENG", "BS1 4PB"),
        ],
    },
    4: {
        "name": "Asia Pacific",
        "country_code": "JP",
        "currency": "JPY",
        "tax_rate": 0.1000,
        "cities": [
            ("Tokyo", "13", "100-0001"),
            ("Osaka", "27", "530-0001"),
            ("Kyoto", "26", "600-8216"),
            ("Yokohama", "14", "220-0011"),
        ],
    },
    5: {
        "name": "South America",
        "country_code": "BR",
        "currency": "BRL",
        "tax_rate": 0.1700,
        "cities": [
            ("São Paulo", "SP", "01000-000"),
            ("Rio de Janeiro", "RJ", "20000-000"),
            ("Curitiba", "PR", "80000-000"),
        ],
    },
    6: {
        "name": "Oceania",
        "country_code": "AU",
        "currency": "AUD",
        "tax_rate": 0.1000,
        "cities": [
            ("Sydney", "NSW", "2000"),
            ("Melbourne", "VIC", "3000"),
            ("Brisbane", "QLD", "4000"),
        ],
    },
    7: {
        "name": "Middle East",
        "country_code": "AE",
        "currency": "AED",
        "tax_rate": 0.0500,
        "cities": [
            ("Dubai", "DXB", "00000"),
            ("Abu Dhabi", "AUH", "00000"),
        ],
    },
    8: {
        "name": "Eastern Europe",
        "country_code": "PL",
        "currency": "PLN",
        "tax_rate": 0.2300,
        "cities": [
            ("Warsaw", "MZ", "00-001"),
            ("Krakow", "MA", "30-001"),
            ("Wroclaw", "DS", "50-001"),
        ],
    },
}

PRODUCT_CATALOG = [
    # Electronics
    ("P1001", "ELEC-SMART-PRO", "Smartphone Pro Max", "Electronics", 899.99, "Electronics Global Direct"),
    ("P1002", "ELEC-LAPTOP-16", "Laptop Pro 16-inch", "Electronics", 1499.00, "Electronics Global Direct"),
    ("P1003", "ELEC-EARBUD-NC", "Wireless Earbuds Active NC", "Electronics", 179.99, "Electronics Global Direct"),
    ("P1004", "ELEC-MONITOR-4K", "4K Ultra-Wide Monitor 32-inch", "Electronics", 499.50, "Electronics Global Direct"),
    ("P1005", "ELEC-WATCH-LTE", "Smart Watch Series 9 LTE", "Electronics", 349.00, "Electronics Global Direct"),
    ("P1006", "ELEC-TAB-AIR", "Tablet Air 11-inch WiFi", "Electronics", 599.00, "Electronics Global Direct"),
    ("P1007", "ELEC-SPK-BT", "Portable Bluetooth Speaker 30W", "Electronics", 89.95, "Electronics Global Direct"),
    # Apparel
    ("P1020", "APP-DNM-JKT", "Classic Denim Jacket", "Apparel", 89.00, "Apparel Global Direct"),
    ("P1021", "APP-CTN-TEE", "Organic Cotton Premium T-Shirt", "Apparel", 29.50, "Apparel Global Direct"),
    ("P1022", "APP-RUN-SHOE", "Ultralight Carbon Running Shoes", "Apparel", 145.00, "Apparel Global Direct"),
    ("P1023", "APP-WOL-SWTR", "Merino Wool Crewneck Sweater", "Apparel", 110.00, "Apparel Global Direct"),
    ("P1024", "APP-ATH-SHRT", "Breathable Athletic Shorts", "Apparel", 38.00, "Apparel Global Direct"),
    ("P1025", "APP-LTH-BELT", "Full-Grain Italian Leather Belt", "Apparel", 54.00, "Apparel Global Direct"),
    # Home & Kitchen
    ("P1040", "HOME-ESP-MCH", "Barista Touch Espresso Machine", "Home & Kitchen", 649.99, "Home Global Direct"),
    ("P1041", "HOME-CKW-SET", "Non-Stick Ceramic Cookware 10-Piece", "Home & Kitchen", 219.00, "Home Global Direct"),
    ("P1042", "HOME-AIR-PUR", "HEPA Smart Air Purifier 500sqft", "Home & Kitchen", 189.99, "Home Global Direct"),
    ("P1043", "HOME-ROB-VAC", "LiDAR Robot Vacuum & Mop Combo", "Home & Kitchen", 429.00, "Home Global Direct"),
    ("P1044", "HOME-BLD-1200", "High-Speed Vortex Blender 1200W", "Home & Kitchen", 129.50, "Home Global Direct"),
    ("P1045", "HOME-CHF-KNF", "Damascus Steel 8-inch Chef Knife", "Home & Kitchen", 95.00, "Home Global Direct"),
    # Sports & Outdoors
    ("P1060", "SPT-YOG-MAT", "High-Density Eco Yoga Mat 6mm", "Sports & Outdoors", 45.00, "Sports Global Direct"),
    ("P1061", "SPT-DMB-SET", "Adjustable Quick-Select Dumbbell 24kg", "Sports & Outdoors", 299.00, "Sports Global Direct"),
    ("P1062", "SPT-TNT-4P", "All-Weather Geodesic Camping Tent 4P", "Sports & Outdoors", 249.00, "Sports Global Direct"),
    ("P1063", "SPT-MTN-BIK", "Trail Hardtail 29er Mountain Bike", "Sports & Outdoors", 899.00, "Sports Global Direct"),
    ("P1064", "SPT-HYD-PCK", "Hydration Daypack with 2L Bladder", "Sports & Outdoors", 65.00, "Sports Global Direct"),
    ("P1065", "SPT-FIT-TRK", "Waterproof GPS Fitness Band", "Sports & Outdoors", 79.99, "Sports Global Direct"),
    # Beauty & Personal
    ("P1080", "BTY-FAC-CRM", "Hydrating Hyaluronic Facial Cream 50ml", "Beauty & Personal", 42.00, "Beauty Global Direct"),
    ("P1081", "BTY-ORG-SHM", "Sulfate-Free Argan Oil Shampoo 500ml", "Beauty & Personal", 24.50, "Beauty Global Direct"),
    ("P1082", "BTY-ELC-TB", "Sonic Electric Toothbrush with UV Case", "Beauty & Personal", 89.00, "Beauty Global Direct"),
    ("P1083", "BTY-SUN-SPF50", "Mineral Sunscreen SPF50+ Invisible", "Beauty & Personal", 28.00, "Beauty Global Direct"),
    ("P1084", "BTY-PRF-EDP", "Artisan Wood & Bergamot Eau de Parfum", "Beauty & Personal", 125.00, "Beauty Global Direct"),
]

PROMOTION_CODES = [
    (None, 0.0, 0.65),           # 65% of orders have no coupon
    ("WELCOME10", 0.10, 0.15),   # 10% off
    ("SAVE15", 0.15, 0.10),      # 15% off
    ("FALL20", 0.20, 0.06),      # 20% off
    ("FREESHIP", 0.00, 0.03),    # Free shipping waiver
    ("VIP25", 0.25, 0.01),       # 25% VIP discount
]

PAYMENT_METHODS = ["credit_card", "apple_pay", "paypal", "google_pay", "klarna_bnpl"]
PAYMENT_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]

CHANNELS = ["iOS App", "Android App", "Web Desktop", "Mobile Safari", "Marketplace"]
CHANNEL_WEIGHTS = [0.35, 0.25, 0.22, 0.12, 0.06]

SHIPPING_CARRIERS = ["FedEx", "UPS", "DHL", "USPS"]
SHIPPING_SERVICES = ["Standard Ground", "Priority 2-Day", "Next Day Air"]

ORDER_STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "SHIPPED", "PENDING", "PROCESSING", "CANCELLED", "RETURNED"]


# ==============================================================================
# Helper Functions: Date Window, Session, Intraday & Seasonality
# ==============================================================================

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
    """Initializes a requests.Session configured with exponential backoff, retry policy, and browser User-Agent."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST", "PUT"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    return session


def get_intraday_offset_seconds(rng: random.Random) -> int:
    """
    Generates a realistic second offset [0, 86400) within the day following
    standard e-commerce customer buying patterns (lunch peak and evening prime-time peak).
    """
    time_blocks = [
        (0, 6, 0.06),    # 00:00 - 06:00 (Night lull)
        (6, 11, 0.16),   # 06:00 - 11:00 (Morning ramp)
        (11, 14, 0.26),  # 11:00 - 14:00 (Lunch peak)
        (14, 18, 0.18),  # 14:00 - 18:00 (Afternoon working hours)
        (18, 22, 0.28),  # 18:00 - 22:00 (Evening prime peak)
        (22, 24, 0.06),  # 22:00 - 24:00 (Late night taper)
    ]
    hours = [b[:2] for b in time_blocks]
    weights = [b[2] for b in time_blocks]
    start_hr, end_hr = rng.choices(hours, weights=weights)[0]
    return rng.randint(start_hr * 3600, min(86399, end_hr * 3600 - 1))


def calculate_daily_volume(target_date: datetime.date, base_volume: int = 400, rng: Optional[random.Random] = None) -> int:
    """
    Calculates realistic day-to-day order volume factoring in day-of-week seasonality:
    - Friday (+25%) & Saturday (+40%) shopping peaks
    - Sunday (+20%) evening rush
    - Midweek baseline (Monday-Wednesday)
    - Organic random Gaussian variance (+/- 10%)
    """
    weekday = target_date.weekday()  # 0 = Monday, 6 = Sunday
    weekday_multipliers = {
        0: 0.92,  # Monday
        1: 0.88,  # Tuesday (lowest volume day)
        2: 0.96,  # Wednesday
        3: 1.05,  # Thursday
        4: 1.25,  # Friday (payday & weekend kick-off)
        5: 1.38,  # Saturday (peak shopping day)
        6: 1.18,  # Sunday (relaxed home browsing)
    }
    multiplier = weekday_multipliers.get(weekday, 1.0)
    variance = rng.uniform(-0.08, 0.08) if rng else random.uniform(-0.08, 0.08)
    computed_volume = int(round(base_volume * (multiplier + variance)))
    return max(150, computed_volume)


# ==============================================================================
# Rich E-Commerce Synthetic Generator
# ==============================================================================

def generate_mock_records(
    start_dt: datetime,
    end_dt: datetime,
    count: Optional[int] = None,
    base_volume: int = 400,
    include_lifecycle_updates: bool = True,
) -> List[Dict[str, Any]]:
    """
    Generates rich, realistic synthetic order transactions strictly timestamped
    within the [start_dt, end_dt) window.

    If count is explicitly supplied, exactly count records are generated.
    If count is None, realistic day-of-week volume is automatically computed.
    """
    seed_val = int(start_dt.timestamp())
    rng = random.Random(seed_val)

    target_date = start_dt.date()
    target_count = count if count is not None else calculate_daily_volume(target_date, base_volume, rng)
    date_str_compact = start_dt.strftime("%Y%m%d")
    total_seconds = int((end_dt - start_dt).total_seconds())

    records: List[Dict[str, Any]] = []

    for idx in range(1, target_count + 1):
        order_id = f"ORD-API-{date_str_compact}-{idx:04d}"
        order_number = f"INV-{date_str_compact}-{idx:04d}"
        cust_id = f"C{rng.randint(10001, 15000)}"
        region_id = rng.randint(1, 8)
        region_info = REGIONS_MASTER[region_id]

        # Multi-item cart generation (1 to 4 distinct items per order)
        num_items = rng.choices([1, 2, 3, 4], weights=[0.60, 0.25, 0.10, 0.05])[0]
        selected_catalog_items = rng.sample(PRODUCT_CATALOG, num_items)

        items: List[Dict[str, Any]] = []
        subtotal = 0.0

        for line_no, prod_tpl in enumerate(selected_catalog_items, start=1):
            pid, sku, name, cat, unit_p, supplier = prod_tpl
            # Slight price variance based on currency/region
            qty = rng.choices([1, 2, 3, 4], weights=[0.65, 0.22, 0.09, 0.04])[0]
            line_total = round(qty * unit_p, 2)
            subtotal += line_total

            items.append({
                "line_item_id": line_no,
                "product_id": pid,
                "sku": sku,
                "product_name": name,
                "category": cat,
                "quantity": qty,
                "unit_price": unit_p,
                "line_total": line_total,
                "supplier_name": supplier,
            })

        subtotal = round(subtotal, 2)

        # Discount code calculation
        promo_options = [p[0] for p in PROMOTION_CODES]
        promo_rates = {p[0]: p[1] for p in PROMOTION_CODES}
        promo_weights = [p[2] for p in PROMOTION_CODES]
        applied_promo = rng.choices(promo_options, weights=promo_weights)[0]
        discount_rate = promo_rates[applied_promo]
        discount_amount = round(subtotal * discount_rate, 2) if applied_promo else 0.0

        # Tax calculation
        tax_rate = region_info["tax_rate"]
        taxable_amount = max(0.0, subtotal - discount_amount)
        tax_amount = round(taxable_amount * tax_rate, 2)

        # Shipping calculation
        shipping_carrier = rng.choice(SHIPPING_CARRIERS)
        shipping_service = rng.choices(SHIPPING_SERVICES, weights=[0.60, 0.30, 0.10])[0]
        if subtotal >= 120.0 or applied_promo == "FREESHIP":
            shipping_fee = 0.00
        elif shipping_service == "Next Day Air":
            shipping_fee = 19.99
        elif shipping_service == "Priority 2-Day":
            shipping_fee = 9.99
        else:
            shipping_fee = 4.99

        total_amount = round(subtotal - discount_amount + tax_amount + shipping_fee, 2)

        # Payment details
        pay_method = rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
        pay_status = "CAPTURED"
        order_status = rng.choice(ORDER_STATUSES)
        if order_status == "CANCELLED":
            pay_status = rng.choice(["REFUNDED", "VOIDED"])
        elif order_status == "PENDING" and rng.random() < 0.03:
            pay_status = "FAILED"
            order_status = "CANCELLED"

        # Timestamp generation adhering to intraday traffic curve
        if total_seconds <= 86400:
            offset_seconds = get_intraday_offset_seconds(rng)
        else:
            offset_seconds = rng.randint(0, total_seconds - 1)

        created_at_dt = start_dt + timedelta(seconds=min(offset_seconds, max(0, total_seconds - 1)))
        created_at_str = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")

        # Destination city & address
        city_tuple = rng.choice(region_info["cities"])
        street_no = rng.randint(100, 9999)
        street_name = rng.choice(["Market St", "Main Ave", "Highland Rd", "Broadway", "Oak Lane", "Commercial Blvd"])
        shipping_address = {
            "street": f"{street_no} {street_name}",
            "city": city_tuple[0],
            "state_province": city_tuple[1],
            "postal_code": city_tuple[2],
            "country_code": region_info["country_code"],
            "region_id": region_id,
        }

        channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
        device = "Mobile" if "App" in channel or "Safari" in channel else "Desktop"

        # Primary item for top-level backward compatibility with existing Bronze/Silver tables
        primary_item = items[0]

        record = {
            # Core primary identifiers
            "order_id": order_id,
            "order_number": order_number,
            "customer_id": cust_id,
            "region_id": region_id,
            "currency": region_info["currency"],
            "order_status": order_status,
            "order_date": created_at_str,
            "created_at": created_at_str,
            "source_system": "REST_API",
            "channel": channel,
            "device": device,

            # Top-level backward-compatible fields (used by 02_Bronze and 03_Silver baseline)
            "product_id": primary_item["product_id"],
            "category": primary_item["category"],
            "quantity": sum(i["quantity"] for i in items),
            "unit_price": primary_item["unit_price"],
            "total_amount": total_amount,

            # Deep e-commerce financial breakdown
            "financials": {
                "subtotal": subtotal,
                "discount_code": applied_promo,
                "discount_amount": discount_amount,
                "tax_rate": tax_rate,
                "tax_amount": tax_amount,
                "shipping_fee": shipping_fee,
                "grand_total": total_amount,
                "item_count": len(items),
            },

            # Multi-line cart items array
            "items": items,

            # Payment details
            "payment": {
                "payment_method": pay_method,
                "payment_status": pay_status,
                "transaction_id": f"txn_{uuid.uuid4().hex[:14]}",
                "is_settled": pay_status == "CAPTURED",
            },

            # Shipping details
            "shipping": {
                "shipping_carrier": shipping_carrier,
                "shipping_service": shipping_service,
                "shipping_fee": shipping_fee,
                "tracking_number": f"TRK{rng.randint(100000000, 999999999)}",
                "shipping_address": shipping_address,
            },
        }

        records.append(record)

    # Optional: Append lifecycle CDC status updates for historical orders
    if include_lifecycle_updates and count is None:
        lifecycle_updates = generate_lifecycle_status_updates(start_dt, count=max(10, int(target_count * 0.08)))
        records.extend(lifecycle_updates)

    logger.info(
        f"Generated {len(records)} mock e-commerce records for window {start_dt} to {end_dt} "
        f"(Day-of-Week: {target_date.strftime('%A')}, Base: {target_count})"
    )
    return records


def generate_lifecycle_status_updates(current_dt: datetime, count: int = 25) -> List[Dict[str, Any]]:
    """
    Generates realistic order status CDC updates for orders placed 1 to 3 days prior.
    Demonstrates Delta Lake MERGE deduplication and state progression in the Silver layer.
    """
    rng = random.Random(int(current_dt.timestamp()) + 777)
    updates: List[Dict[str, Any]] = []

    for idx in range(1, count + 1):
        days_ago = rng.choice([1, 2, 3])
        past_date = current_dt - timedelta(days=days_ago)
        past_date_str = past_date.strftime("%Y%m%d")
        order_idx = rng.randint(1, 150)
        order_id = f"ORD-API-{past_date_str}-{order_idx:04d}"

        # Transition status
        new_status = rng.choices(["PROCESSING", "SHIPPED", "DELIVERED", "RETURNED"], weights=[0.20, 0.40, 0.30, 0.10])[0]

        offset_seconds = rng.randint(0, 86399)
        updated_ts = (current_dt + timedelta(seconds=offset_seconds)).strftime("%Y-%m-%d %H:%M:%S")

        cust_id = f"C{rng.randint(10001, 15000)}"
        prod_tpl = rng.choice(PRODUCT_CATALOG)
        reg_id = rng.randint(1, 8)
        reg_info = REGIONS_MASTER[reg_id]

        update_record = {
            "order_id": order_id,
            "order_number": f"INV-{past_date_str}-{order_idx:04d}",
            "customer_id": cust_id,
            "product_id": prod_tpl[0],
            "category": prod_tpl[3],
            "region_id": reg_id,
            "currency": reg_info["currency"],
            "quantity": 1,
            "unit_price": prod_tpl[4],
            "total_amount": prod_tpl[4],
            "order_status": new_status,
            "order_date": past_date.strftime("%Y-%m-%d %H:%M:%S"),
            "created_at": updated_ts,
            "source_system": "REST_API_CDC",
            "is_lifecycle_update": True,
            "lifecycle_transition": f"STATUS_UPDATE_TO_{new_status}",
        }
        updates.append(update_record)

    logger.info(f"Generated {len(updates)} CDC order lifecycle transition updates.")
    return updates


# ==============================================================================
# Live Public REST API Integration (Frankfurter FX & DummyJSON)
# ==============================================================================

def fetch_frankfurter_fx(target_date_str: str) -> Optional[Dict[str, Any]]:
    """
    Fetches real European Central Bank daily foreign exchange rates from the free,
    open Frankfurter API (https://api.frankfurter.app/{target_date}).
    """
    session = build_resilient_session(retries=2)
    # Frankfurter publishes on business days; use target date or 'latest'
    url = f"https://api.frankfurter.app/{target_date_str}?from=USD"
    logger.info(f"Querying Frankfurter FX API: {url}")
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            logger.info(f"Successfully fetched live FX rates for {data.get('date')}: {len(data.get('rates', {}))} currencies.")
            return {
                "base_currency": data.get("base", "USD"),
                "date": data.get("date", target_date_str),
                "rates": data.get("rates", {}),
                "source": "European Central Bank (Frankfurter API)",
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            }
        else:
            logger.warning(f"Frankfurter API returned status {resp.status_code}")
            return None
    except Exception as exc:
        logger.warning(f"Unable to query Frankfurter FX API: {exc}")
        return None


def fetch_dummyjson_orders(start_dt: datetime, end_dt: datetime, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetches live real e-commerce cart transactions from DummyJSON public REST API
    and conforms them into our Lakehouse schema.
    """
    session = build_resilient_session(retries=3)
    url = f"https://dummyjson.com/carts?limit={limit}"
    logger.info(f"Querying DummyJSON Carts endpoint: {url}")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"DummyJSON returned status {resp.status_code}")
            return []

        payload = resp.json()
        carts = payload.get("carts", [])
        records: List[Dict[str, Any]] = []
        date_str_compact = start_dt.strftime("%Y%m%d")

        for idx, cart in enumerate(carts, start=1):
            order_id = f"ORD-LIVE-{date_str_compact}-{idx:04d}"
            user_id = cart.get("userId", 1)
            cust_id = f"C{10000 + user_id}"
            reg_id = ((user_id - 1) % 8) + 1
            reg_info = REGIONS_MASTER[reg_id]

            items = []
            for item_no, p in enumerate(cart.get("products", []), start=1):
                items.append({
                    "line_item_id": item_no,
                    "product_id": f"P{p.get('id', 1000) + 1000}",
                    "sku": f"LIVE-SKU-{p.get('id')}",
                    "product_name": p.get("title", "Product"),
                    "category": "Electronics",
                    "quantity": p.get("quantity", 1),
                    "unit_price": round(float(p.get("price", 10.0)), 2),
                    "line_total": round(float(p.get("total", 10.0)), 2),
                })

            primary_item = items[0] if items else {
                "product_id": "P1001",
                "category": "Electronics",
                "quantity": 1,
                "unit_price": 49.99,
            }

            subtotal = round(float(cart.get("total", 50.0)), 2)
            discount_total = round(float(cart.get("discountedTotal", subtotal)), 2)
            discount_amt = max(0.0, round(subtotal - discount_total, 2))
            total_amt = discount_total

            created_at_dt = start_dt + timedelta(seconds=random.randint(0, 86399))
            created_at_str = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")

            records.append({
                "order_id": order_id,
                "order_number": f"INV-{date_str_compact}-{idx:04d}",
                "customer_id": cust_id,
                "product_id": primary_item["product_id"],
                "category": primary_item["category"],
                "region_id": reg_id,
                "currency": reg_info["currency"],
                "quantity": cart.get("totalQuantity", 1),
                "unit_price": primary_item["unit_price"],
                "total_amount": total_amt,
                "order_status": "COMPLETED",
                "order_date": created_at_str,
                "created_at": created_at_str,
                "source_system": "DummyJSON_Live",
                "channel": "Web Desktop",
                "items": items,
                "financials": {
                    "subtotal": subtotal,
                    "discount_amount": discount_amt,
                    "grand_total": total_amt,
                    "item_count": len(items),
                },
                "payment": {
                    "payment_method": "credit_card",
                    "payment_status": "CAPTURED",
                    "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                },
                "shipping": {
                    "shipping_carrier": "FedEx",
                    "shipping_service": "Standard Ground",
                    "tracking_number": f"TRK{random.randint(100000000, 999999999)}",
                },
            })

        logger.info(f"Conformed {len(records)} live transactions from DummyJSON.")
        return records

    except Exception as exc:
        logger.warning(f"DummyJSON extraction failed: {exc}")
        return []


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
        "User-Agent": "Enterprise-Extract-Agent/2.0",
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
            if isinstance(payload, list):
                batch = payload
                has_more = len(batch) >= page_size
            elif isinstance(payload, dict):
                batch = payload.get("data") or payload.get("records") or payload.get("results") or payload.get("orders") or []
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


# ==============================================================================
# Partitioned File Writing & Databricks Volume Upload
# ==============================================================================

def write_partitioned_json(
    records: List[Dict[str, Any]],
    output_base_dir: str,
    date_str: str,
    newline_delimited: bool = True,
    file_prefix: str = "orders",
) -> Path:
    """
    Writes records into a partitioned directory structure:
    <output_base_dir>/year=YYYY/month=MM/day=DD/<file_prefix>_YYYY-MM-DD.json
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    partition_dir = (
        Path(output_base_dir)
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"{file_prefix}_{date_str}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        if newline_delimited:
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
    host = databricks_host.rstrip("/")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"https://{host}"

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


# ==============================================================================
# CLI Entrypoint
# ==============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Extract previous day's rich e-commerce records in JSON format via REST API")
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
        help="API Base URL, or 'mock', 'live', 'dummyjson', 'frankfurter'. If empty or 'mock', enhanced synthesizer is used.",
    )
    parser.add_argument(
        "--base-volume",
        type=int,
        default=int(os.environ.get("BASE_VOLUME", "400")),
        help="Base order volume for synthetic daily generation before day-of-week seasonality (default: 400).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Explicit fixed record count override (bypasses seasonality).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.environ.get("OUTPUT_DIR", "./landing_data/api_orders"),
        help="Base directory for JSON output landing.",
    )
    parser.add_argument(
        "--env",
        type=str,
        default=os.environ.get("TARGET_ENV", "dev").lower(),
        choices=["dev", "prod", "qa"],
        help="Target environment (dev | prod | qa). Defaults to dev.",
    )
    parser.add_argument(
        "--include-lifecycle",
        action="store_true",
        default=os.environ.get("INCLUDE_LIFECYCLE", "true").lower() in ("true", "1", "yes"),
        help="Whether to generate order lifecycle CDC updates for previous days (default: true).",
    )
    parser.add_argument(
        "--include-fx-rates",
        action="store_true",
        default=os.environ.get("INCLUDE_FX_RATES", "true").lower() in ("true", "1", "yes"),
        help="Whether to fetch and save daily foreign exchange rates (default: true).",
    )
    parser.add_argument(
        "--volume-target-path",
        type=str,
        default=os.environ.get("DATABRICKS_VOLUME_PATH"),
        help="Target Unity Catalog Volume path in Databricks.",
    )
    parser.add_argument(
        "--upload-to-volume",
        action="store_true",
        default=os.environ.get("UPLOAD_TO_VOLUME", "false").lower() in ("true", "1", "yes"),
        help="Whether to upload output JSON to Databricks Volume via REST API.",
    )
    parser.add_argument(
        "--summary-file",
        type=str,
        default=os.environ.get("SUMMARY_FILE", "metrics/latest_run_summary.json"),
        help="Path to write the execution summary JSON file.",
    )

    args = parser.parse_args()

    start_dt, end_dt, date_str = calculate_date_window(args.date)

    api_token = os.environ.get("API_KEY") or os.environ.get("API_TOKEN") or os.environ.get("API_BEARER_TOKEN")
    api_key_header = os.environ.get("API_KEY_HEADER")

    records: List[Dict[str, Any]] = []
    api_mode = args.api_url.lower() if args.api_url else "mock"

    if api_mode in ("dummyjson", "live"):
        logger.info("Attempting extraction from live public API (DummyJSON)...")
        records = fetch_dummyjson_orders(start_dt, end_dt, limit=args.count or args.base_volume)
        if not records:
            logger.warning("Live DummyJSON API returned 0 records. Falling back to enhanced simulation engine.")
            records = generate_mock_records(
                start_dt=start_dt,
                end_dt=end_dt,
                count=args.count,
                base_volume=args.base_volume,
                include_lifecycle_updates=args.include_lifecycle,
            )
    elif args.api_url and api_mode not in ("mock", "frankfurter"):
        logger.info(f"Connecting to custom REST API endpoint: {args.api_url}")
        try:
            records = extract_from_api(
                base_url=args.api_url,
                start_dt=start_dt,
                end_dt=end_dt,
                api_token=api_token,
                api_key_header=api_key_header,
            )
        except Exception as exc:
            logger.warning(f"External API extraction failed: {exc}. Falling back to enhanced simulation engine.")
            records = generate_mock_records(
                start_dt=start_dt,
                end_dt=end_dt,
                count=args.count,
                base_volume=args.base_volume,
                include_lifecycle_updates=args.include_lifecycle,
            )
    else:
        logger.info("Running in enhanced enterprise e-commerce simulation mode.")
        records = generate_mock_records(
            start_dt=start_dt,
            end_dt=end_dt,
            count=args.count,
            base_volume=args.base_volume,
            include_lifecycle_updates=args.include_lifecycle,
        )

    if not records:
        logger.warning(f"No records found for date {date_str}.")
        return 0

    # Write partitioned newline-delimited JSON (NDJSON)
    output_file = write_partitioned_json(
        records=records,
        output_base_dir=args.output_dir,
        date_str=date_str,
        newline_delimited=True,
        file_prefix="orders",
    )

    # Optional: Fetch and save daily FX rates companion file
    if args.include_fx_rates:
        fx_data = fetch_frankfurter_fx(date_str)
        if fx_data:
            fx_dir = Path(args.output_dir).parent / "fx_rates"
            fx_file = write_partitioned_json(
                records=[fx_data],
                output_base_dir=str(fx_dir),
                date_str=date_str,
                newline_delimited=False,
                file_prefix="fx_rates",
            )
            logger.info(f"Daily foreign exchange rates landed at: {fx_file}")

    # Resolve volume target dynamically based on environment
    volume_target = args.volume_target_path or f"bronze_{args.env}/ecommerce/raw_landing/api_orders"
    logger.info(f"Target environment: {args.env.upper()} | Target Volume: {volume_target}")

    # If running directly inside Databricks runtime where /Volumes/ exists on OS
    direct_volume_path = Path("/Volumes") / volume_target
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

    # Push to Databricks Volume via REST API if configured
    db_host = os.environ.get("DATABRICKS_HOST")
    db_token = os.environ.get("DATABRICKS_TOKEN")
    upload_status = "STAGED_LOCAL"

    if args.upload_to_volume or (db_host and db_token):
        if db_host and db_token:
            rel_partition = f"year={start_dt.year:04d}/month={start_dt.month:02d}/day={start_dt.day:02d}/{output_file.name}"
            remote_volume_dest = f"{volume_target.rstrip('/')}/{rel_partition}"
            upload_success = upload_to_databricks_volume(
                local_file_path=output_file,
                databricks_host=db_host,
                databricks_token=db_token,
                volume_path=remote_volume_dest,
            )
            upload_status = "SUCCESS" if upload_success else "FAILED"
        else:
            logger.warning("Upload to volume requested but DATABRICKS_HOST or DATABRICKS_TOKEN is missing.")
            upload_status = "STAGED_LOCAL"

    file_size_bytes = output_file.stat().st_size
    if file_size_bytes < 1024:
        file_size_fmt = f"{file_size_bytes} B"
    elif file_size_bytes < 1024 * 1024:
        file_size_fmt = f"{file_size_bytes / 1024:.2f} KB"
    else:
        file_size_fmt = f"{file_size_bytes / (1024 * 1024):.2f} MB"

    if args.summary_file:
        try:
            summary_path = Path(args.summary_file)
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "target_date": date_str,
                "environment": args.env,
                "record_count": len(records),
                "file_size_bytes": file_size_bytes,
                "file_size_formatted": file_size_fmt,
                "volume_target": volume_target,
                "upload_status": upload_status,
                "file_name": output_file.name,
                "relative_path": str(output_file),
            }
            with open(summary_path, "w", encoding="utf-8") as sf:
                json.dump(summary_data, sf, indent=2)
            logger.info(f"Execution summary saved to {summary_path}")
        except Exception as exc:
            logger.warning(f"Failed to write summary file: {exc}")

    logger.info(f"Pipeline extraction stage complete for {date_str}. Total records processed: {len(records)} ({file_size_fmt})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

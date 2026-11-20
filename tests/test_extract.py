import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from extract_api import (
    build_resilient_session,
    calculate_date_window,
    calculate_daily_volume,
    generate_mock_records,
    generate_lifecycle_status_updates,
    fetch_frankfurter_fx,
    write_partitioned_json,
)


class TestExtractAPI(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_calculate_date_window_custom_date(self):
        start_dt, end_dt, date_str = calculate_date_window("2026-10-22")
        self.assertEqual(date_str, "2026-10-22")
        self.assertEqual(start_dt, datetime(2026, 10, 22, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(end_dt, datetime(2026, 10, 23, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual((end_dt - start_dt).total_seconds(), 86400)

    def test_calculate_date_window_yesterday(self):
        start_dt, end_dt, date_str = calculate_date_window("yesterday")
        self.assertEqual((end_dt - start_dt).total_seconds(), 86400)
        self.assertEqual(start_dt.tzinfo, timezone.utc)
        self.assertRegex(date_str, r"^\d{4}-\d{2}-\d{2}$")

    def test_generate_mock_records_schema_and_timestamps(self):
        start_dt = datetime(2026, 10, 20, 0, 0, 0, tzinfo=timezone.utc)
        end_dt = datetime(2026, 10, 21, 0, 0, 0, tzinfo=timezone.utc)
        count = 50
        records = generate_mock_records(start_dt, end_dt, count=count)

        self.assertEqual(len(records), count)
        for r in records:
            # Backward-compatibility required fields
            self.assertIn("order_id", r)
            self.assertIn("customer_id", r)
            self.assertIn("product_id", r)
            self.assertIn("region_id", r)
            self.assertIn("quantity", r)
            self.assertIn("unit_price", r)
            self.assertIn("total_amount", r)
            self.assertIn("order_status", r)
            self.assertIn("created_at", r)

            # Check timestamp is within target date window
            ts = datetime.strptime(r["created_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            self.assertTrue(start_dt <= ts < end_dt, f"Timestamp {ts} outside window [{start_dt}, {end_dt})")

    def test_rich_e_commerce_schema_attributes(self):
        start_dt = datetime(2026, 10, 22, 0, 0, 0, tzinfo=timezone.utc)
        end_dt = datetime(2026, 10, 23, 0, 0, 0, tzinfo=timezone.utc)
        records = generate_mock_records(start_dt, end_dt, count=25)

        for r in records:
            # Check rich nested structure
            self.assertIn("order_number", r)
            self.assertIn("channel", r)
            self.assertIn("device", r)
            self.assertIn("currency", r)
            self.assertIn("items", r)
            self.assertIn("financials", r)
            self.assertIn("payment", r)
            self.assertIn("shipping", r)

            # Check items cart array
            self.assertIsInstance(r["items"], list)
            self.assertGreaterEqual(len(r["items"]), 1)
            first_item = r["items"][0]
            self.assertIn("line_item_id", first_item)
            self.assertIn("product_id", first_item)
            self.assertIn("sku", first_item)
            self.assertIn("unit_price", first_item)
            self.assertIn("quantity", first_item)
            self.assertIn("line_total", first_item)

            # Check payment details
            pay = r["payment"]
            self.assertIn(pay["payment_method"], ["credit_card", "apple_pay", "paypal", "google_pay", "klarna_bnpl"])
            self.assertIn("payment_status", pay)
            self.assertIn("transaction_id", pay)

            # Check shipping details & address
            ship = r["shipping"]
            self.assertIn(ship["shipping_carrier"], ["FedEx", "UPS", "DHL", "USPS"])
            self.assertIn("tracking_number", ship)
            self.assertIn("shipping_address", ship)
            addr = ship["shipping_address"]
            self.assertIn("city", addr)
            self.assertIn("country_code", addr)
            self.assertEqual(addr["region_id"], r["region_id"])

            # Check financials calculations
            fin = r["financials"]
            self.assertIn("subtotal", fin)
            self.assertIn("tax_amount", fin)
            self.assertIn("grand_total", fin)
            self.assertEqual(fin["grand_total"], r["total_amount"])

    def test_day_of_week_seasonality(self):
        # Tuesday: typical low-volume day (weekday = 1)
        tue_dt = datetime(2026, 11, 17, 0, 0, 0, tzinfo=timezone.utc)
        # Saturday: typical high-volume day (weekday = 5)
        sat_dt = datetime(2026, 11, 21, 0, 0, 0, tzinfo=timezone.utc)

        tue_vol = calculate_daily_volume(tue_dt.date(), base_volume=400)
        sat_vol = calculate_daily_volume(sat_dt.date(), base_volume=400)

        # Saturday should be noticeably higher volume than Tuesday
        self.assertGreater(sat_vol, tue_vol)
        self.assertGreaterEqual(sat_vol, 480)
        self.assertLessEqual(tue_vol, 430)

    def test_lifecycle_status_cdc_generation(self):
        curr_dt = datetime(2026, 11, 20, 0, 0, 0, tzinfo=timezone.utc)
        updates = generate_lifecycle_status_updates(curr_dt, count=15)

        self.assertEqual(len(updates), 15)
        for u in updates:
            self.assertTrue(u.get("is_lifecycle_update"))
            self.assertIn(u["order_status"], ["PROCESSING", "SHIPPED", "DELIVERED", "RETURNED"])
            self.assertIn("lifecycle_transition", u)
            self.assertIn("created_at", u)

    def test_frankfurter_fx_fetch(self):
        # Test live or resilient fallback of ECB exchange rates
        fx = fetch_frankfurter_fx("2026-10-22")
        if fx is not None:
            self.assertEqual(fx["base_currency"], "USD")
            self.assertIn("rates", fx)
            self.assertGreaterEqual(len(fx["rates"]), 10)
            self.assertIn("EUR", fx["rates"])

    def test_write_partitioned_ndjson(self):
        start_dt = datetime(2026, 10, 22, 0, 0, 0, tzinfo=timezone.utc)
        end_dt = datetime(2026, 10, 23, 0, 0, 0, tzinfo=timezone.utc)
        records = generate_mock_records(start_dt, end_dt, count=10)

        out_path = write_partitioned_json(records, self.test_dir, "2026-10-22", newline_delimited=True)
        self.assertTrue(out_path.exists())

        # Verify partition path
        expected_part = Path(self.test_dir) / "year=2026" / "month=10" / "day=22" / "orders_2026-10-22.json"
        self.assertEqual(out_path.resolve(), expected_part.resolve())

        # Verify NDJSON lines
        with open(out_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        self.assertEqual(len(lines), 10)
        parsed = json.loads(lines[0])
        self.assertEqual(parsed["order_id"], records[0]["order_id"])
        self.assertIn("items", parsed)

    def test_env_parameter_routing(self):
        from extract_api import main
        import sys

        test_args = ["extract_api.py", "--date", "2026-10-22", "--env", "dev", "--output-dir", self.test_dir, "--count", "50"]
        orig_argv = sys.argv
        try:
            sys.argv = test_args
            exit_code = main()
            self.assertEqual(exit_code, 0)

            expected_file = Path(self.test_dir) / "year=2026" / "month=10" / "day=22" / "orders_2026-10-22.json"
            self.assertTrue(expected_file.exists())
        finally:
            sys.argv = orig_argv

    def test_build_resilient_session(self):
        session = build_resilient_session()
        self.assertIsNotNone(session.adapters.get("https://"))
        self.assertIsNotNone(session.adapters.get("http://"))


if __name__ == "__main__":
    unittest.main()

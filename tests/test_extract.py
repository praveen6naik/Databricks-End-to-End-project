import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from extract_api import (
    build_resilient_session,
    calculate_date_window,
    generate_mock_records,
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
            # Check required fields
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

    def test_env_parameter_routing(self):
        # Verify default env and custom env volume destination
        from extract_api import main
        import sys
        
        # Test CLI argument parsing with --env dev
        test_args = ["extract_api.py", "--date", "2026-10-22", "--env", "dev", "--output-dir", self.test_dir]
        orig_argv = sys.argv
        try:
            sys.argv = test_args
            exit_code = main()
            self.assertEqual(exit_code, 0)
            
            # Check partition file exists
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

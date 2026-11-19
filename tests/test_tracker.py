import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from update_tracker import (
    format_bytes,
    generate_svg_area_chart,
    generate_svg_bar_chart,
    load_history,
    merge_run_summary,
    render_html_dashboard,
    save_history,
)


class TestUpdateTracker(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_format_bytes(self):
        self.assertEqual(format_bytes(500), "500 B")
        self.assertEqual(format_bytes(1024), "1.00 KB")
        self.assertEqual(format_bytes(1536), "1.50 KB")
        self.assertEqual(format_bytes(1048576), "1.00 MB")
        self.assertEqual(format_bytes(1073741824), "1.00 GB")

    def test_load_and_save_history(self):
        history_file = Path(self.test_dir) / "test_history.json"
        # Test loading non-existent
        self.assertEqual(load_history(history_file), [])

        # Test saving and loading
        sample = [{"target_date": "2026-11-18", "record_count": 250, "file_size_bytes": 79688}]
        save_history(history_file, sample)
        loaded = load_history(history_file)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["record_count"], 250)

    def test_merge_run_summary_append_and_update(self):
        history = [
            {
                "run_id": "run-01",
                "timestamp": "2026-11-18T10:00:00Z",
                "target_date": "2026-11-17",
                "environment": "prod",
                "record_count": 250,
                "file_size_bytes": 80000,
                "file_size_formatted": "78.12 KB",
                "volume_target": "bronze_prod/ecommerce/raw_landing/api_orders",
                "upload_status": "SUCCESS",
                "trigger_type": "schedule",
                "file_name": "orders_2026-11-17.json",
            }
        ]

        # 1. Append a new date
        summary_new = {
            "target_date": "2026-11-18",
            "environment": "prod",
            "record_count": 300,
            "file_size_bytes": 95000,
            "volume_target": "bronze_prod/ecommerce/raw_landing/api_orders",
            "upload_status": "SUCCESS",
            "file_name": "orders_2026-11-18.json",
        }
        updated_history = merge_run_summary(history, summary_new, run_id="run-02", trigger_type="schedule")
        self.assertEqual(len(updated_history), 2)
        self.assertEqual(updated_history[1]["record_count"], 300)

        # 2. Update existing run with same run_id
        summary_update = {
            "target_date": "2026-11-18",
            "environment": "prod",
            "record_count": 350,
            "file_size_bytes": 105000,
            "volume_target": "bronze_prod/ecommerce/raw_landing/api_orders",
            "upload_status": "SUCCESS",
            "file_name": "orders_2026-11-18.json",
        }
        updated_history2 = merge_run_summary(updated_history, summary_update, run_id="run-02", trigger_type="schedule")
        self.assertEqual(len(updated_history2), 2)
        self.assertEqual(updated_history2[1]["record_count"], 350)

    def test_render_html_dashboard_content(self):
        history = [
            {
                "run_id": "run-01",
                "timestamp": "2026-11-18T10:00:00Z",
                "target_date": "2026-11-17",
                "environment": "prod",
                "record_count": 250,
                "file_size_bytes": 80000,
                "file_size_formatted": "78.12 KB",
                "volume_target": "bronze_prod/ecommerce/raw_landing/api_orders",
                "upload_status": "SUCCESS",
                "trigger_type": "schedule",
                "file_name": "orders_2026-11-17.json",
            }
        ]
        html_output = render_html_dashboard(history, datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc))

        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("Databricks Lakehouse", html_output)
        self.assertIn("Upload Volume Tracker", html_output)
        self.assertIn("250", html_output)
        self.assertIn("78.12 KB", html_output)
        self.assertIn("SUCCESS", html_output)
        self.assertIn("svg", html_output)
        self.assertIn("uploadsTable", html_output)

    def test_svg_chart_generation(self):
        data_records = [("2026-11-17", 250), ("2026-11-18", 300)]
        bar_svg = generate_svg_bar_chart(data_records)
        self.assertIn("<svg", bar_svg)
        self.assertIn("<rect", bar_svg)

        data_size = [("2026-11-17", 78.12), ("2026-11-18", 95.5)]
        area_svg = generate_svg_area_chart(data_size)
        self.assertIn("<svg", area_svg)
        self.assertIn("<path", area_svg)
        self.assertIn("<circle", area_svg)


if __name__ == "__main__":
    unittest.main()

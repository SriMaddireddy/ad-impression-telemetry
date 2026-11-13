import unittest
from scripts.verification import verify_window


class WindowVerificationTests(unittest.TestCase):
    def setUp(self):
        self.expected = dict(impressions=960, clicks=30, conversions=10,
                             spend=1.92, revenue=50.0)

    def test_accepts_floating_point_aggregation_roundoff(self):
        actual = dict(self.expected, spend=1.920000000000001)
        verify_window(("campaign-002", 5000), actual, self.expected)

    def test_rejects_correct_counts_with_wrong_money(self):
        for field in ("spend", "revenue"):
            with self.subTest(field=field):
                actual = dict(self.expected)
                actual[field] += 0.01
                with self.assertRaisesRegex(ValueError, field):
                    verify_window("window", actual, self.expected)

    def test_rejects_nonfinite_money(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "revenue"):
                    verify_window("window", dict(self.expected, revenue=value), self.expected)

    def test_rejects_missing_and_duplicate_count_effects(self):
        for count in (959, 961):
            with self.subTest(count=count):
                with self.assertRaisesRegex(ValueError, "impressions"):
                    verify_window("window", dict(self.expected, impressions=count), self.expected)

    def test_requires_money_fields(self):
        actual = dict(self.expected)
        del actual["spend"]
        with self.assertRaises(KeyError):
            verify_window("window", actual, self.expected)

class LoadCommandTests(unittest.TestCase):
    def run_load(self, corrupt=False):
        import contextlib
        import io
        import json
        import runpy
        import sys
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        windows = {}
        def response(request, timeout):
            path = request.full_url.split("8090", 1)[1]
            if path == "/api/stats":
                value = dict(mode="fixture")
            elif path == "/api/events":
                events = json.loads(request.data)
                for event in events:
                    start = event["eventTimeMs"] // 5000 * 5000
                    key = (event["campaignId"], start)
                    row = windows.setdefault(key, dict(
                        campaignId=key[0], windowStart=start, impressions=0,
                        clicks=0, conversions=0, spend=0.0, revenue=0.0))
                    if event["type"] == "IMPRESSION":
                        row["impressions"] += 1
                        row["spend"] += event["amount"]
                    elif event["type"] == "CLICK":
                        row["clicks"] += 1
                    else:
                        row["conversions"] += 1
                        row["revenue"] += event["amount"]
                value = dict(accepted=len(events))
            else:
                value = list(windows.values())
                if corrupt:
                    value[0]["spend"] += 1
            return io.StringIO(json.dumps(value))

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            arguments = ["scripts/load.py", "--count", "1100", "--output", str(output)]
            with patch.object(sys, "argv", arguments), patch.object(
                sys, "path", [str(Path("scripts").resolve())] + sys.path
            ), patch("urllib.request.urlopen", side_effect=response), patch(
                "time.sleep"
            ), contextlib.redirect_stdout(io.StringIO()):
                if corrupt:
                    with self.assertRaisesRegex(ValueError, "spend"):
                        runpy.run_path("scripts/load.py", run_name="__main__")
                    self.assertFalse(output.exists())
                else:
                    runpy.run_path("scripts/load.py", run_name="__main__")
                    report = json.loads(output.read_text())
                    self.assertEqual(report["verifiedEvents"], 1100)
                    self.assertEqual(report["acknowledgedEvents"], 1100)
                    self.assertIn("revenue", report["verifiedFields"])

    def test_command_verifies_multiple_batches_without_counting_money_as_events(self):
        self.run_load()

    def test_command_refuses_report_on_money_corruption(self):
        self.run_load(corrupt=True)

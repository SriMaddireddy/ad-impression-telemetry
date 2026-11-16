import unittest
from unittest.mock import patch
from scripts.redis_checks import sink_lua, verify_projection


class SinkLuaTests(unittest.TestCase):
    def test_loads_production_retention_and_upsert(self):
        script = sink_lua()
        self.assertIn("redis.call('HSET'", script)
        self.assertIn("redis.call('ZADD'", script)
        self.assertIn("redis.call('ZCARD'", script)
        self.assertIn("redis.call('HDEL'", script)

    def test_missing_source_contract_fails_explicitly(self):
        with patch("pathlib.Path.read_text", return_value="class Changed {}"):
            with self.assertRaisesRegex(ValueError, "Cannot locate"):
                sink_lua()

    def test_failed_verification_cleans_isolated_keys(self):
        calls = []
        def command(arguments):
            calls.append(arguments)
            return "unexpected"
        with self.assertRaises(AssertionError):
            verify_projection(command)
        self.assertEqual(calls[-1][5], "DEL")
        keys = calls[-1][6:]
        self.assertEqual(len(keys), 2)
        self.assertTrue(all(key.startswith("{telemetry-check-") for key in keys))
        self.assertNotIn("{telemetry}:windows", keys)

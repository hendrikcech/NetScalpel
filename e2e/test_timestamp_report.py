import copy
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace

import verify


class TimestampReportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = pathlib.Path(self.directory.name) / "info.json"
        self.info = {
            "status": "complete",
            "timestamp_report": {"tx": {
                "requested": "software",
                "source": "software",
                "packet_count": 3,
                "missing_count": 0, "invalid_count": 0,
            }},
        }

    def check_report(self, info):
        self.path.write_text(json.dumps(info))
        return verify.cmd_txts(SimpleNamespace(info=[str(self.path)]))

    def test_complete_software_evidence(self):
        self.assertEqual(self.check_report(self.info), [])

    def test_selection_not_request_determines_evidence(self):
        info = copy.deepcopy(self.info)
        tx = info["timestamp_report"]["tx"]
        tx["requested"] = "hardware"
        tx["selection_reason"] = "hardware TX timestamping is not implemented"
        self.assertEqual(self.check_report(info), [])

        tx["requested"] = "software"
        tx["source"] = "app"
        tx["selection_reason"] = "kernel software timestamping is not implemented for this protocol"
        self.assertTrue(self.check_report(info))

    def test_missing_or_incomplete_evidence(self):
        for field, value in [
            ("source", "app"), ("source", "hardware"),
            ("packet_count", 0), ("missing_count", 1),
            ("invalid_count", 1),
        ]:
            with self.subTest(field=field, value=value):
                info = copy.deepcopy(self.info)
                info["timestamp_report"]["tx"][field] = value
                self.assertTrue(self.check_report(info))
        info = copy.deepcopy(self.info)
        info["status"] = "failed"
        self.assertTrue(self.check_report(info))
        for info in [{}, [], {"timestamp_report": None}]:
            self.assertTrue(self.check_report(info))

    def test_unreadable_report(self):
        self.assertTrue(verify.cmd_txts(SimpleNamespace(info=[str(self.path)])))
        self.path.write_text("not json")
        self.assertTrue(verify.cmd_txts(SimpleNamespace(info=[str(self.path)])))


if __name__ == "__main__":
    unittest.main()

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from measurements.repeat import run_range


class RepetitionTest(unittest.TestCase):
    def test_stale_output_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("measurements.repeat.subprocess.run") as call:
                with self.assertRaises(FileExistsError):
                    run_range(["measure"], [1], Path(directory) / "execution.json", fresh_root=directory)
            call.assert_not_called()

    def test_failure_with_valid_artifacts_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def measure(command, **kwargs):
                # A failed run can still leave plausible artifacts behind.
                (root / f"{command[-1]}.csv").write_text("otherwise valid retained data")
                return subprocess.CompletedProcess(command, 1 if command[-1] == "2" else 0)

            with patch("measurements.repeat.subprocess.run", side_effect=measure) as call:
                self.assertEqual(run_range(["measure"], range(1, 4), root / "execution.json"), 1)
            self.assertEqual(call.call_count, 3)
            record = json.loads((root / "execution.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual([run["status"] for run in record["runs"]], ["complete", "failed", "complete"])
            self.assertTrue((root / "2.csv").exists())

    def test_success_and_launch_failure(self):
        for error in (None, OSError("missing executable")):
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "execution.json"
                with patch("measurements.repeat.subprocess.run", side_effect=error,
                           return_value=subprocess.CompletedProcess([], 0)):
                    self.assertEqual(run_range(["measure"], [0], path), 0 if error is None else 1)
                self.assertEqual(json.loads(path.read_text())["status"], "complete" if error is None else "failed")


if __name__ == "__main__":
    unittest.main()

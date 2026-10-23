import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from benchmarks import report, run, throughput
from test_analyze import START, artifact, write_artifact


def udp_csv(path, delays=(20, 999)):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost", "dup_count"])
        for i, delay in enumerate(delays):
            # Each packet starts at an integral second; delays stay below 1s.
            writer.writerow([i, f"2026-10-23T10:00:{i:02d}Z",
                             f"2026-10-23T10:00:{i:02d}.{delay:03d}000000Z", delay, 1408, "false", 0])


class ReportTest(unittest.TestCase):
    def assert_outputs(self, out):
        for name in ("manifest.json", "metrics.json", "summary.csv", "report.md", "plot.svg"):
            self.assertTrue((out / name).is_file(), name)
        ET.parse(out / "plot.svg")
        self.assertIn("not_evaluated", (out / "report.md").read_text())

    def test_pacing_repeated_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for i, packets in enumerate((2, 99)):
                record, rows = artifact(name=f"sample{i}.csv", start=START + i, packets=packets)
                write_artifact(root, record, rows)
            out = root / "report"
            result = report.generate("pacing", root, out, cases=["single7pps"], repetitions=2)
            self.assert_outputs(out)
            self.assertEqual(result["validity"]["status"], "valid")
            self.assertEqual(result["execution"]["status"], "unknown")
            self.assertEqual(len(result["provenance"]["raw_sidecars"]), 2)
            metrics = json.loads((out / "metrics.json").read_text())
            self.assertEqual([r["metrics"]["packets"] for r in metrics["runs"]], [2, 99])
            self.assertEqual(metrics["aggregate"]["single7pps"]["packets"]["median"], 50.5)
            with (out / "summary.csv").open() as stream:
                self.assertEqual(len(list(csv.DictReader(stream))),
                                 len(report.flatten(metrics["runs"][0]["metrics"])) * 3)

    def test_report_never_overwrites_directory_inputs_or_symlinked_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record, rows = artifact(name="summary.csv")
            write_artifact(root, record, rows)
            original = (root / "summary.csv").read_bytes()
            with self.assertRaisesRegex(ValueError, "overwrite the input"):
                report.generate("pacing", root, root, cases=["single7pps"], repetitions=1)
            out = root / "report"
            out.mkdir()
            (out / "summary.csv").symlink_to(root / "summary.csv")
            with self.assertRaisesRegex(ValueError, "overwrite the input"):
                report.generate("pacing", root, out, cases=["single7pps"], repetitions=1)
            self.assertEqual((root / "summary.csv").read_bytes(), original)

    def test_pacing_orphan_csv_is_invalid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record, rows = artifact()
            write_artifact(root, record, rows)
            (root / "incomplete.csv").write_text("partial raw")
            result = report.generate("pacing", root, root / "report", cases=["single7pps"], repetitions=1)
            self.assertEqual(result["validity"]["status"], "invalid")
            self.assertIn("raw CSV set differs", " ".join(result["validity"]["reasons"]))

    def test_empty_and_invalid_not_vacuously_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for suite in ("pacing", "udp"):
                out = root / suite
                result = report.generate(suite, root, out)
                self.assertEqual(result["validity"]["status"], "invalid")
                self.assert_outputs(out)
            (root / "bad.csv.json").write_text('{"kernel": false}')
            result = report.generate("pacing", root, root / "pacing")
            self.assertEqual(result["validity"]["status"], "invalid")
            self.assertEqual(len(result["artifacts"]["raw"]), 1)

    def test_udp_all_csvs_independent_and_outliers_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            udp_csv(root / "a" / "same.csv")
            udp_csv(root / "b" / "same.csv", (20, 21))
            out = root / "report"
            # An existing generated CSV must never be reinterpreted as raw.
            out.mkdir()
            (out / "extra.csv").write_text("not raw")
            for _ in range(2):
                result = report.generate("udp", root, out, expected_delay_ms=20)
                self.assert_outputs(out)
                self.assertEqual(result["validity"]["status"], "valid")
                self.assertEqual(result["provenance"]["timestamps"]["source"], "unknown")
                metrics = json.loads((out / "metrics.json").read_text())
                self.assertEqual([r["file"] for r in metrics["runs"]], ["a/same.csv", "b/same.csv"])
                self.assertEqual(metrics["runs"][0]["metrics"]["owd_ms"]["max"], 999)
                self.assertEqual(metrics["runs"][0]["metrics"]["error_ms"]["max"], 979)
                self.assertEqual(metrics["aggregate"]["udp"]["owd_ms.max"]["n"], 2)

    def test_udp_invalid_independent_run_and_no_received(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            udp_csv(root / "good.csv")
            (root / "bad.csv").write_text("bad\n")
            result = report.generate("udp", root, root / "report")
            self.assertEqual(result["validity"]["status"], "invalid")
            metrics = json.loads((root / "report" / "metrics.json").read_text())
            self.assertEqual(len(metrics["runs"]), 2)
            self.assertEqual(metrics["aggregate"]["udp"]["owd_ms.max"]["n"], 1)
            (root / "lost.csv").write_text("seq,ts_sent,ts_rcvd,owd_ms,size,lost,dup_count\n"
                                           "0,2026-10-23T10:00:00Z,,,1408,true,0\n")
            result = report.generate("udp", root / "lost.csv", root / "lost-report")
            self.assertIn("no received packets", " ".join(result["validity"]["reasons"]))

    def test_udp_expected_population_and_artifact_set(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            udp_csv(root / "client/owd_ul.csv")
            result = report.generate("udp", root / "client", root / "report",
                                     min_packets=3, expected_csv=["owd_ul.csv", "owd_dl.csv"])
            self.assertEqual(result["validity"]["status"], "invalid")
            self.assertIn("CSV artifact set differs", " ".join(result["validity"]["reasons"]))
            self.assertIn("only 2 rows", " ".join(result["validity"]["reasons"]))
            udp_csv(root / "client/owd_dl.csv")
            result = report.generate("udp", root / "client", root / "report",
                                     min_packets=2, expected_csv=["owd_ul.csv", "owd_dl.csv"])
            self.assertEqual(result["validity"]["status"], "valid")

    def test_tx_log_positive_fallback_and_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            udp_csv(root / "raw.csv")
            log = root / "tx.log"
            for text, valid in (("selectMsgsSent: returning with txTsReader infos", True),
                                ("", False),
                                ("selectMsgsSent: returning with txTsReader infos\nusing timestamps from run function", False)):
                log.write_text(text)
                result = report.generate("udp", root / "raw.csv", root / "report", tx_logs=[log])
                self.assertEqual(result["validity"]["status"], "valid" if valid else "invalid")
                self.assertEqual(result["provenance"]["timestamps"]["source"], "kernel software TX" if valid else "unknown")
            log.unlink()
            result = report.generate("udp", root / "raw.csv", root / "report", tx_logs=[log])
            self.assertEqual(result["validity"]["status"], "invalid")

    def test_cli_direct_from_other_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            udp_csv(root / "raw.csv")
            process = subprocess.run([sys.executable, str(report.ROOT / "benchmarks/report.py"),
                                      "udp", str(root / "raw.csv"), "--out", str(root / "report"),
                                      "--execution-status", "complete", "--backend", "nixnet"],
                                     cwd=root, capture_output=True, text=True)
            self.assertEqual(process.returncode, 0, process.stderr)
            result = json.loads((root / "report/manifest.json").read_text())
            self.assertEqual(result["execution"]["status"], "complete")
            self.assertEqual(result["backend"], "nixnet")

    def test_udp_run_metadata_and_summary_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            udp_csv(root / "client/raw.csv")
            (root / "run.json").write_text('{"invocation": ["nix", "run"], "git_revision": "abc"}')
            result = report.generate("udp", root / "client", root / "report")
            self.assertEqual(result["provenance"]["run_metadata"][0]["data"]["git_revision"], "abc")
            result = report.generate("udp", root / "report/summary.csv", root / "bad-report")
            self.assertEqual(result["validity"]["status"], "invalid")
            original = (root / "report/summary.csv").read_bytes()
            with self.assertRaisesRegex(ValueError, "overwrite the input"):
                report.generate("udp", root / "report/summary.csv", root / "report")
            self.assertEqual((root / "report/summary.csv").read_bytes(), original)

    def test_continuous_windows_boundaries_and_no_dynamic_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record, rows = artifact("single100", points=[(0, 0), (999999, 0), (1000000, 0),
                                                        (1199999999, 0), (1200000000, 0)])
            write_artifact(root, record, rows)
            result = report.generate("pacing", root, root / "report", cases=["single100"], repetitions=1)
            self.assertEqual(result["validity"]["status"], "valid")
            metrics = json.loads((root / "report/metrics.json").read_text())
            self.assertEqual(metrics["runs"][0]["metrics"]["tx_windows"]["1ms"]["windows"], 1200)
            with (root / "report/throughput.csv").open() as stream:
                series = list(csv.DictReader(stream))
            one_ms = [r for r in series if r["window_ms"] == "1"]
            self.assertEqual([int(r["packets"]) for r in one_ms[:2]], [2, 1])
            self.assertEqual(sum(int(r["packets"]) for r in one_ms), 4)
            self.assertEqual(float(one_ms[0]["udp_mbps"]), 2 * 1408 * 8 / 1000)
            ET.parse(root / "report/throughput.svg")
            record["phases"][0]["duration_ms"] = 1201
            windows, _ = throughput.windows(record, root)
            self.assertEqual(windows["60ms"]["windows"], 20)
            record["case"] = "equal100"
            self.assertEqual(throughput.windows(record, root), ({}, []))


class RunnerTest(unittest.TestCase):
    def test_stale_directory_rejected_without_invocation(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(run.subprocess, "run") as call:
            with self.assertRaises(FileExistsError):
                run.run_pacing(directory, 1, ["single7pps"])
            call.assert_not_called()

    def test_process_failure_and_launch_failure_still_report(self):
        for error in (None, OSError("no go executable")):
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory:
                out = Path(directory) / "new"
                with patch.object(run.subprocess, "run", side_effect=error,
                                  return_value=subprocess.CompletedProcess([], 2)):
                    self.assertEqual(run.run_pacing(out, 1, ["single7pps"]), 1)
                result = json.loads((out / "report/manifest.json").read_text())
                self.assertEqual(result["execution"]["status"], "failed")
                self.assertEqual(result["validity"]["status"], "invalid")
                self.assertTrue((out / "go-bench.txt").exists())

    def test_successful_execution_can_have_invalid_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "new"
            with patch.object(run.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as call:
                self.assertEqual(run.run_pacing(out, 2, ["single7pps", "short7pps"]), 1)
            result = json.loads((out / "report/manifest.json").read_text())
            self.assertEqual(result["execution"]["status"], "complete")
            self.assertEqual(result["validity"]["status"], "invalid")
            argv = call.call_args.args[0]
            self.assertIn("^BenchmarkRateSender$/^(single7pps|short7pps)$", argv)
            self.assertIn("-benchtime=1x", argv)
            self.assertNotIn("NETSCALPEL_PACING_CASE", call.call_args.kwargs["env"])

    def test_successful_valid_run(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "new"

            def fake(command, **kwargs):
                raw = Path(kwargs["env"]["NETSCALPEL_PACING_STUDY"])
                record, rows = artifact()
                write_artifact(raw, record, rows)
                return subprocess.CompletedProcess(command, 0)

            with patch.object(run.subprocess, "run", side_effect=fake):
                self.assertEqual(run.run_pacing(out, 1, ["single7pps"]), 0)
            result = json.loads((out / "report/manifest.json").read_text())
            self.assertEqual(result["validity"]["status"], "valid")
            self.assertTrue(result["artifacts"]["runner"])


if __name__ == "__main__":
    unittest.main()

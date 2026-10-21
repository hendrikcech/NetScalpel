import contextlib
import copy
import csv
import importlib.util
import io
import json
from pathlib import Path
import statistics
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("analyze", Path(__file__).with_name("analyze.py"))
analyze = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analyze)

START = 1_700_000_000_000_000_000


def artifact(case="single7pps", packets=2, points=None, name="sample.csv", start=START):
    """Construct independently derived, internally consistent synthetic data."""
    if points is None:
        points = [(1_000_000 + i * 100, 0) for i in range(packets)]
    phases, nominal = [], 0
    for i, (pps, ms) in enumerate(analyze.schedule(case)):
        selected = [(n, elapsed) for n, (elapsed, segment) in enumerate(points) if segment == i]
        count = len(selected)
        achieved = count * 1000 / ms
        phases.append(dict(segment=i, pps=pps, duration_ms=ms, nominal_ms=nominal,
                           expected_packets=pps * ms // 1000, observed=True, packets=count,
                           start_ms=nominal, drift_ms=0.,
                           first_ms=selected[0][1] / 1e6 if count else 0.,
                           last_ms=selected[-1][1] / 1e6 if count else 0.,
                           span_ms=(selected[-1][1] - selected[0][1]) / 1e6 if count else 0.,
                           mbps=achieved * 1408 * 8 / 1e6, achieved_pps=achieved, budget_ms=ms,
                           boundary_gap_us=(selected[0][1] - points[selected[0][0] - 1][0]) / 1000
                           if count and selected[0][0] else None))
        nominal += ms
    gaps = [(b[0] - a[0]) / 1000 for a, b in zip(points, points[1:])]
    baseline = []
    if case in ("single500", "single100"):
        for ms in range(100, 1101, 20):
            for a, b in zip(points, points[1:]):
                if a[0] < ms * 1_000_000 <= b[0]:
                    baseline.append((b[0] - a[0]) / 1000)
                    break
    record = dict(schema=1, case=case, kernel=True, packets=len(points), phases=phases,
                  baseline_gaps_us=baseline,
                  gap_median_us=statistics.median(gaps) if gaps else 0.,
                  gap_p99_us=sorted(gaps)[max(0, (len(gaps) * 99 + 99) // 100 - 1)] if gaps else 0.,
                  gap_stddev_us=statistics.pstdev(gaps) if gaps else 0.,
                  csv=name, metadata=dict(created_utc="2026-10-21T10:00:00.000000001Z",
                      start_unix_ns=start, go_version="go1.26", goos="linux", goarch="amd64",
                      gomaxprocs=2, cpus=4, go_environment=dict(GOFLAGS="", GOGC="", GOMEMLIMIT=""),
                      git_revision="abc", git_status="", source_path="udp.go", source_sha256="a" * 64,
                      benchmark_sha256="c" * 64, kernel_version="Linux", argv=[], suite_version=1,
                      legacy_prefix=False, capped=case != "d2_uncapped", payload_bytes=1408,
                      timestamp_source="complete kernel software TX"))
    rows = [[i, start + elapsed, elapsed, segment, 1408] for i, (elapsed, segment) in enumerate(points)]
    return record, rows


def sample(case="single7pps", packets=2):
    return artifact(case, packets)[0]


def write_artifact(directory, record, rows, sidecar=None):
    with (directory / record["csv"]).open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "kernel_tx_unix_ns", "elapsed_ns", "segment", "udp_bytes"])
        writer.writerows(rows)
    (directory / (sidecar or record["csv"] + ".json")).write_text(json.dumps(record))


class AnalysisTest(unittest.TestCase):
    def test_median_range_not_mean(self):
        records = [sample(packets=n) for n in (1, 2, 99)]
        self.assertEqual(analyze.summarize(records)["single7pps"]["packets"],
                         dict(median=2, min=1, max=99, n=3))

    def test_empty_and_nonfinite(self):
        self.assertEqual(analyze.distribution([])["median"], None)
        with self.assertRaises(ValueError):
            analyze.distribution([float("nan")])

    def test_d2_pulse_budgets(self):
        result = analyze.run_metrics(sample("d2_capped"))
        self.assertEqual(result["pulse_expected_packets"], 50 * (500_000_000 // 8 // 1408 * 8 // 1000))
        self.assertEqual(result["pulse_observed"], 50)
        self.assertEqual(result["pulse_packets"], 2)
        self.assertEqual(result["final_packets"], 0)

    def test_missing_phase_not_zero_drift(self):
        record = sample()
        record["phases"][-1]["observed"] = False
        metrics = analyze.run_metrics(record)
        self.assertNotIn("final_drift_ms", metrics)
        self.assertEqual(metrics["unobserved_active_segments"], 1)

    def test_baseline_and_boundary(self):
        record = sample("equal500")
        record["baseline_gaps_us"] = [1, 2, 9]
        record["phases"][1]["boundary_gap_us"] = 10
        metrics = analyze.run_metrics(record)
        self.assertEqual(metrics["baseline_gap_median_us"], 2)
        self.assertEqual(metrics["baseline_gap_p99_us"], 9)
        self.assertEqual(metrics["final_boundary_gap_us"], 10)

    def test_comparison(self):
        before = analyze.summarize([sample(packets=2)])
        after = analyze.summarize([sample(packets=5)])
        self.assertEqual(analyze.compare(before, after)["single7pps"]["packets"], 3)
        with self.assertRaises(ValueError):
            analyze.compare(before, {})

    def test_valid_all_schedules_empty_and_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            for case in analyze.CASES:
                for count in (0, 1, 2):
                    record, rows = artifact(case, count)
                    write_artifact(directory, record, rows)
                    analyze.validate(record, directory)
            record, rows = artifact("short7pps", 0)
            self.assertEqual(record["phases"][0]["budget_ms"], 100)
            record["phases"][-1].update(observed=False, budget_ms=0, start_ms=0)
            write_artifact(directory, record, rows)
            analyze.validate(record, directory)

    def test_strict_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact()
            write_artifact(directory, record, rows)
            analyze.validate(record, directory)
            for change in (lambda r: r.update(kernel=False),
                           lambda r: r.update(case="unknown"),
                           lambda r: r["metadata"].pop("git_revision"),
                           lambda r: r.update(packets=3),
                           lambda r: r["phases"][0].update(pps=8),
                           lambda r: r.update(csv="../escape.csv")):
                changed = copy.deepcopy(record)
                change(changed)
                with self.assertRaises((ValueError, KeyError)):
                    analyze.validate(changed, directory)

    def test_tampered_derived_statistics(self):
        changes = {
            "gap_p99": lambda r: r.update(gap_p99_us=987654321),
            "gap_median": lambda r: r.update(gap_median_us=3),
            "gap_stddev": lambda r: r.update(gap_stddev_us=3),
            "span": lambda r: r["phases"][0].update(span_ms=2),
            "first": lambda r: r["phases"][0].update(first_ms=2),
            "last": lambda r: r["phases"][0].update(last_ms=2),
            "rate": lambda r: r["phases"][0].update(mbps=2),
            "pps": lambda r: r["phases"][0].update(achieved_pps=2),
            "budget": lambda r: r["phases"][0].update(budget_ms=100),
            "drift": lambda r: r["phases"][0].update(drift_ms=1),
            "nonfinite": lambda r: r["phases"][0].update(start_ms=float("nan")),
        }
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact()
            self.assertEqual(record["gap_p99_us"], .1)
            write_artifact(directory, record, rows)
            for name, change in changes.items():
                with self.subTest(name=name):
                    changed = copy.deepcopy(record)
                    change(changed)
                    with self.assertRaises(ValueError):
                        analyze.validate(changed, directory)

    def test_wrong_elapsed_one_nanosecond(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact()
            rows[-1][2] += 1
            write_artifact(directory, record, rows)
            with self.assertRaisesRegex(ValueError, "elapsed_ns"):
                analyze.validate(record, directory)

    def test_nonmonotonic_segment_assignment(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact("split7pps", points=[(100, 0), (200, 1), (300, 0)])
            write_artifact(directory, record, rows)
            with self.assertRaisesRegex(ValueError, "kernel CSV"):
                analyze.validate(record, directory)

    def test_boundary_gap_and_empty_phase_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact("split7pps", points=[(100, 0), (600_000_001, 1)])
            write_artifact(directory, record, rows)
            analyze.validate(record, directory)
            record["phases"][1]["boundary_gap_us"] += .01
            with self.assertRaisesRegex(ValueError, "boundary_gap"):
                analyze.validate(record, directory)
            record, rows = artifact("short7pps", 0)
            write_artifact(directory, record, rows)
            for key, value in (("budget_ms", 0), ("span_ms", .01), ("mbps", 1), ("boundary_gap_us", 0)):
                changed = copy.deepcopy(record)
                changed["phases"][0][key] = value
                with self.subTest(key=key), self.assertRaises(ValueError):
                    analyze.validate(changed, directory)

    def test_baseline_crossing_recomputed(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact("single500", points=[(99_999_900, 0), (100_000_000, 0),
                                                        (120_000_100, 0), (1_100_000_001, 0)])
            write_artifact(directory, record, rows)
            analyze.validate(record, directory)
            record["baseline_gaps_us"][0] += .01
            with self.assertRaisesRegex(ValueError, "virtual crossing"):
                analyze.validate(record, directory)
            record["baseline_gaps_us"] = []
            with self.assertRaisesRegex(ValueError, "crossing gap count"):
                analyze.validate(record, directory)

    def test_load_repetition_and_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            record, rows = artifact()
            write_artifact(directory, record, rows)
            self.assertEqual(len(analyze.load(directory, ["single7pps"], 1)), 1)
            with self.assertRaisesRegex(ValueError, "expected 6"):
                analyze.load(directory, ["single7pps"], 6)
            other, rows = artifact(name="other.csv", start=START + 10)
            other["metadata"]["source_sha256"] = "b" * 64
            write_artifact(directory, other, rows)
            with self.assertRaisesRegex(ValueError, "mixed build/runtime"):
                analyze.load(directory, ["single7pps"], 2)

    def test_duplicate_csv_and_sidecar_names(self):
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                record, rows = artifact(name="a.csv")
                write_artifact(directory, record, rows)
                if not duplicate:
                    (directory / "a.csv.json").unlink()
                (directory / "z.csv.json").write_text(json.dumps(record))
                with self.assertRaisesRegex(ValueError, "duplicate CSV" if duplicate else "sidecar name"):
                    analyze.load(directory, ["single7pps"], 2 if duplicate else 1)

    def test_duplicate_run_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            for name in ("a.csv", "b.csv"):
                record, rows = artifact(name=name)
                write_artifact(directory, record, rows)
            with self.assertRaisesRegex(ValueError, "duplicate run identity"):
                analyze.load(directory, ["single7pps"], 2)

    def test_within_directory_tuning_consistency(self):
        for key, value in (("cpus", 8), ("go_environment", dict(GOFLAGS="", GOGC="50", GOMEMLIMIT=""))):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                record, rows = artifact(name="a.csv")
                write_artifact(directory, record, rows)
                record, rows = artifact(name="b.csv", start=START + 10)
                record["metadata"][key] = value
                write_artifact(directory, record, rows)
                with self.assertRaisesRegex(ValueError, "mixed build/runtime"):
                    analyze.load(directory, ["single7pps"], 2)

    def test_environment_comparison_and_explicit_override(self):
        for key in analyze.ENVIRONMENT_KEYS:
            before = sample()["metadata"]
            after = copy.deepcopy(before)
            after[key] = "different"
            self.assertEqual(set(analyze.environment_differences(before, after)), {key})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for label, gc in (("before", ""), ("after", "50")):
                destination = root / label
                destination.mkdir()
                record, rows = artifact()
                record["metadata"]["go_environment"]["GOGC"] = gc
                write_artifact(destination, record, rows)
            argv = [str(root / "before"), str(root / "after"), "--cases", "single7pps", "--repetitions", "1"]
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                analyze.main(argv)
            self.assertEqual(error.exception.code, 1)
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
                analyze.main(argv + ["--allow-environment-differences", "--json", str(root / "report.json")])
            self.assertIn("WARNING", stderr.getvalue())
            report = json.loads((root / "report.json").read_text())
            self.assertTrue(report["environment_differences_allowed"])
            self.assertEqual(report["environment_differences"]["go_environment"]["candidate"]["GOGC"], "50")


if __name__ == "__main__":
    unittest.main()

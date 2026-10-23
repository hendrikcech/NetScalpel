import contextlib
import csv
import importlib.util
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("verify", ROOT / "e2e/verify.py")
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class VerifyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "udp.csv"

    def fixture(self, delays):
        with self.path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost", "dup_count"])
            for i, delay in enumerate(delays):
                # All sends share the same software timestamp, as can happen in batches.
                writer.writerow([i, "2026-01-01T00:00:00Z",
                                 f"2026-01-01T00:00:00.{int(delay * 1e6):09d}Z",
                                 delay, 100, "false", 0])

    def check(self, profile="smoke", **kwargs):
        args = dict(csv=str(self.path), delay_ms=10, eps_ms=2, eps_below_ms=1,
                    max_lost=0, min_packets=1, profile=profile,
                    max_outlier_fraction=.01, max_excess_ms=50)
        args.update(kwargs)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            errors = verify.cmd_owd(SimpleNamespace(**args))
        return errors, output.getvalue()

    def test_one_late_packet_smoke_passes_strict_fails(self):
        self.fixture([10] * 99 + [12.5])
        errors, output = self.check()
        self.assertFalse(errors)
        self.assertIn("outliers=1/100", output)
        self.assertIn("outlier_seqs=[99]", output)
        self.assertTrue(self.check("strict")[0])

    def test_systematic_offsets_and_tail_limits(self):
        for delays in ([13] * 100, [8] + [10] * 99, [10] * 98 + [13] * 2,
                       [10] * 99 + [61]):
            with self.subTest(delays=delays[-2:]):
                self.fixture(delays)
                self.assertTrue(self.check()[0])

    def test_all_lost_fails_even_when_allowed(self):
        self.path.write_text("seq,ts_sent,ts_rcvd,owd_ms,size,lost,dup_count\n"
                             "0,2026-01-01T00:00:00Z,,,100,true,0\n")
        self.assertTrue(self.check(max_lost=1)[0])

    def test_zero_loss_default_and_strict_valid(self):
        self.fixture([10] * 100)
        self.assertFalse(self.check("strict")[0])
        with self.path.open(newline="") as f:
            data = list(csv.reader(f))
        data[-1][2:4] = ["", ""]
        data[-1][5] = "true"
        with self.path.open("w", newline="") as f:
            csv.writer(f).writerows(data)
        self.assertTrue(self.check()[0])

    def test_log_requires_positive_evidence_and_no_fallback(self):
        for content, success in (("", False), ("unrelated log", False),
                                 (verify.TXTS_SUCCESS_MARKER, True),
                                 (verify.TXTS_SUCCESS_MARKER + " using timestamps from run function", False)):
            self.path.write_text(content)
            with contextlib.redirect_stdout(io.StringIO()):
                errors = verify.cmd_txts(SimpleNamespace(log=[str(self.path)]))
            self.assertEqual(not errors, success)

    def test_cli_validation_and_direct_invocation(self):
        self.fixture([10])
        base = [sys.executable, str(ROOT / "e2e/verify.py"), "owd", "--csv", str(self.path), "--delay-ms", "10"]
        self.assertEqual(subprocess.run(base, cwd=self.temp.name, capture_output=True).returncode, 0)
        for flag, value in (("--eps-ms", "nan"), ("--max-outlier-fraction", "1.1"),
                            ("--max-excess-ms", "-1"), ("--min-packets", "0"),
                            ("--max-lost", "-1"), ("--delay-ms", "inf")):
            with self.subTest(flag=flag):
                result = subprocess.run(base + [flag, value], capture_output=True)
                self.assertEqual(result.returncode, 2, result.stderr)

    def test_environment_profile_and_override(self):
        self.fixture([10] * 99 + [12.5])
        base = [sys.executable, str(ROOT / "e2e/verify.py"), "owd", "--csv", str(self.path), "--delay-ms", "10"]
        env = dict(os.environ, NETSCALPEL_TEST_PROFILE="strict")
        self.assertEqual(subprocess.run(base, env=env, capture_output=True).returncode, 1)
        self.assertEqual(subprocess.run(base + ["--profile", "smoke"], env=env, capture_output=True).returncode, 0)
        env["NETSCALPEL_TEST_PROFILE"] = "invalid"
        self.assertEqual(subprocess.run(base, env=env, capture_output=True).returncode, 2)

    def test_rate_smoke_accepts_slow_run_and_tail_strict_rejects(self):
        self.fixture([10, 11, 400])
        args = SimpleNamespace(csv=str(self.path), delay_ms=10, min_packets=3,
                               rate_mbit=1, overhead_bytes=42, limit=100,
                               tolerance=.1, profile="smoke")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(verify.cmd_rate(args))
            args.profile = "strict"
            self.assertTrue(verify.cmd_rate(args))
            args.profile = "smoke"
            args.rate_mbit = .0001
            self.assertTrue(verify.cmd_rate(args))
        self.fixture([1, 2, 3])
        args.rate_mbit = 1
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(verify.cmd_rate(args))

    def test_loss_broad_smoke_and_statistical_strict(self):
        self.fixture([10] * 1000)
        with self.path.open(newline="") as f:
            data = list(csv.reader(f))
        for row in data[1:401]:
            row[2:4] = ["", ""]
            row[5] = "true"
        with self.path.open("w", newline="") as f:
            csv.writer(f).writerows(data)
        args = SimpleNamespace(csv=str(self.path), loss_percent=30, z=4,
                               min_packets=1000, profile="smoke", smoke_loss_tolerance=.15)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(verify.cmd_loss(args))
            args.profile = "strict"
            self.assertTrue(verify.cmd_loss(args))
            args.profile = "smoke"
            args.loss_percent = 0
            self.assertTrue(verify.cmd_loss(args))

    def test_loss_smoke_catches_missing_impairment(self):
        self.fixture([10] * 1000)
        args = SimpleNamespace(csv=str(self.path), loss_percent=5, z=4,
                               min_packets=1000, profile="smoke", smoke_loss_tolerance=.03)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(verify.cmd_loss(args))

    def test_rate_smoke_catches_unshaped_offered_load(self):
        self.fixture([10, 10.01, 10.02])
        args = SimpleNamespace(csv=str(self.path), delay_ms=10, min_packets=3,
                               rate_mbit=100, overhead_bytes=0, limit=100,
                               tolerance=.1, profile="smoke")
        # Three 100-byte packets over 20us = 120Mbps: broad smoke accepts.
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(verify.cmd_rate(args))
            # But the same offered rate cannot pass a 50Mbps bottleneck check.
            args.rate_mbit = 50
            self.assertTrue(verify.cmd_rate(args))

    def test_rate_smoke_catches_catastrophic_loss(self):
        self.fixture([10 + i * .01 for i in range(1000)])
        with self.path.open(newline="") as f:
            data = list(csv.reader(f))
        for row in data[3:]:
            row[2:4] = ["", ""]
            row[5] = "true"
        with self.path.open("w", newline="") as f:
            csv.writer(f).writerows(data)
        args = SimpleNamespace(csv=str(self.path), delay_ms=10, min_packets=1000,
                               rate_mbit=50, overhead_bytes=42, limit=200,
                               tolerance=.1, profile="smoke")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(any("10%" in error for error in verify.cmd_rate(args)))

    def tcp_fixture(self, rtt=20, acked=1000, invalid=None):
        with self.path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Time", "State", "CAState", "RTT", "MinRTT", "ThruBytesAcked", "BBRMaxBW"])
            writer.writerow(["2026-01-01T00:00:00Z", "ESTABLISHED", "Open", rtt, rtt, 0, ""])
            writer.writerow(["2026-01-01T00:00:01Z", "ESTABLISHED", "Open", invalid or rtt, rtt, acked, ""])

    def test_tcp_smoke_accepts_low_throughput_strict_rejects(self):
        self.tcp_fixture()
        args = SimpleNamespace(csv=str(self.path), min_rows=2, rtt_ms=20, rate_mbit=10, profile="smoke")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(verify.cmd_tcp(args))
            args.profile = "strict"
            self.assertTrue(verify.cmd_tcp(args))
            args.profile = "smoke"
            for kwargs in (dict(rtt=1), dict(rtt=1000), dict(acked=0), dict(acked=10_000_000),
                           dict(invalid="NaN"), dict(invalid="-1")):
                self.tcp_fixture(**kwargs)
                self.assertTrue(verify.cmd_tcp(args), kwargs)
            self.tcp_fixture(acked=1_250_000)
            args.profile = "strict"
            self.assertFalse(verify.cmd_tcp(args))

    def test_tcp_empty_malformed_and_too_short(self):
        args = SimpleNamespace(csv=str(self.path), min_rows=2, rtt_ms=20, rate_mbit=10, profile="smoke")
        for content in ("", "Time,RTT\nbad,20\nbad,20\n", "Time,RTT\nbad,nan\n"):
            self.path.write_text(content)
            self.assertTrue(verify.cmd_tcp(args))

    def test_tcp_smoke_requires_transfer_progress(self):
        self.tcp_fixture(acked=1)
        with self.path.open(newline="") as f:
            data = list(csv.reader(f))
        data[1][5] = "1"
        with self.path.open("w", newline="") as f:
            csv.writer(f).writerows(data)
        args = SimpleNamespace(csv=str(self.path), min_rows=2, rtt_ms=20, rate_mbit=10, profile="smoke")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(verify.cmd_tcp(args))


if __name__ == "__main__":
    unittest.main()

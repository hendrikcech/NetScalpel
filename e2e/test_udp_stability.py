"""Deterministic analyzer tests; no privileges, sockets or timing assertions."""

import csv
from pathlib import Path
import struct
import tempfile
import unittest

import udp_stability as bench


class StabilityTests(unittest.TestCase):
    def test_nanosecond_and_timezone_parsing(self):
        self.assertEqual(
            bench.timestamp_ns("1970-01-01T00:00:01.123456789Z"), 1123456789
        )
        self.assertEqual(
            bench.timestamp_ns("1970-01-01T01:00:01.123456789+01:00"), 1123456789
        )
        self.assertEqual(bench.timestamp_ns("1970-01-01T00:00:00Z"), 0)

    def test_windows_include_empty_and_exclude_partial(self):
        packets = [(0, 100, 100), (1, 10_000_100, 100), (2, 20_000_100, 100)]
        self.assertEqual(
            bench.window_rates(packets, 100, 25_000_000, 10_000_000), [0.08, 0.08]
        )
        self.assertEqual(
            bench.window_rates(packets[:1], 100, 20_000_000, 10_000_000), [0.08, 0]
        )
        self.assertEqual(bench.longest_run([0, 0, 1, 0], lambda x: x == 0), 2)

    def test_pause_and_catchup_is_not_hidden_by_average(self):
        # 1 packet/ms; 60ms stalled, then a single catch-up burst.
        packets = [
            (i, (i if i < 20 or i >= 80 else 80) * 1_000_000, 1408) for i in range(200)
        ]
        rates = bench.window_rates(packets, 0, 200_000_000, 10_000_000)
        self.assertEqual(sum(rates) / len(rates), 0.011264 * 1000)
        self.assertEqual(bench.longest_run(rates, lambda x: x == 0), 6)
        self.assertGreater(max(rates), 6 * sum(rates) / len(rates))

    def write_pcap(self, path, packets, endian="<", nano=True, link=1):
        magic = 0xA1B23C4D if nano else 0xA1B2C3D4
        with open(path, "wb") as stream:
            stream.write(struct.pack(endian + "IHHIIII", magic, 2, 4, 0, 0, 128, link))
            for seq, ts, size in packets:
                if link == 1:
                    l2 = bytes(12) + b"\x08\x00"
                elif link == 113:
                    l2 = bytes(14) + b"\x08\x00"
                elif link == 276:
                    l2 = b"\x08\x00" + bytes(18)
                else:
                    l2 = b""
                ip = (
                    b"\x45\x00"
                    + struct.pack("!H", size + 28)
                    + bytes(5)
                    + b"\x11"
                    + bytes(10)
                )
                udp = struct.pack("!HHHHQ", 12345, 23456, size + 8, 0, seq)
                frame = l2 + ip + udp
                sec, ns = divmod(ts, 10**9)
                stream.write(
                    struct.pack(
                        endian + "IIII",
                        sec,
                        ns if nano else ns // 1000,
                        len(frame),
                        size + 42,
                    )
                )
                stream.write(frame)

    def test_pcap_formats(self):
        packets = [(0, 1234567890, 1408), (1, 1234567999, 1408)]
        with tempfile.TemporaryDirectory() as directory:
            for endian in ("<", ">"):
                for nano in (True, False):
                    for link in (1, 101, 113, 276):
                        path = Path(directory) / "sample.pcap"
                        self.write_pcap(path, packets, endian, nano, link)
                        expected = (
                            packets
                            if nano
                            else [(s, t // 1000 * 1000, n) for s, t, n in packets]
                        )
                        self.assertEqual(bench.read_pcap(path), expected)
            path.write_bytes(b"not pcap")
            with self.assertRaises(ValueError):
                bench.read_pcap(path)

    def test_matched_capture_and_invalid_loss(self):
        packets = [(i, 10**9 + i * 1_000_000, 1408) for i in range(200)]
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            pcap, csv_path, log = [
                directory / name
                for name in ("sample.pcap", "sample.csv", "capture.log")
            ]
            log.write_text("200 packets captured\n0 packets dropped by kernel\n")
            with open(csv_path, "w") as stream:
                writer = csv.writer(stream)
                writer.writerow(["seq", "ts_sent", "size"])
                for seq, ts, size in packets:
                    writer.writerow(
                        [seq, f"1970-01-01T00:00:01.{ts % 10**9:09d}Z", size]
                    )
            self.write_pcap(pcap, packets)
            report, valid, metrics = bench.analyze(
                pcap, csv_path, 11.2, 200, [10], 0.1, log
            )
            self.assertTrue(valid)
            self.assertIn("max bin difference=0.000", report)
            self.assertAlmostEqual(metrics["first60_percent"], 100)
            self.write_pcap(pcap, packets[:40] + packets[41:])
            report, valid, _ = bench.analyze(pcap, csv_path, 11.2, 200, [10], 0.1, log)
            self.assertFalse(valid)
            self.assertIn("missing/duplicate/unexpected=1/0/0", report)
            self.write_pcap(pcap, packets + packets[:1])
            self.assertFalse(
                bench.analyze(pcap, csv_path, 11.2, 200, [10], 0.1, log)[1]
            )
            log.write_text("no drop statistics\n")
            self.write_pcap(pcap, packets)
            self.assertFalse(
                bench.analyze(pcap, csv_path, 11.2, 200, [10], 0.1, log)[1]
            )
            log.write_text("1 packets dropped by kernel\n")
            self.assertFalse(
                bench.analyze(pcap, csv_path, 11.2, 200, [10], 0.1, log)[1]
            )

    def test_quantization_warning(self):
        rates = bench.window_rates(
            [(i, i * 1_121_076, 1408) for i in range(892)], 0, 10**9, 10**6
        )
        # Even near-perfect spacing at 10Mbps gives 100% 1ms outliers.
        self.assertTrue(
            all(abs(rate / (892 * 1408 * 8 / 1e6) - 1) > 0.1 for rate in rates)
        )


if __name__ == "__main__":
    unittest.main()

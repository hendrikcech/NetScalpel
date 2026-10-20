"""Capture-oracle regression tests, including plausible-but-wrong CSVs."""

import csv
from pathlib import Path
import struct
import tempfile
import unittest

import udp_delivery as delivery


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.csv = self.root / "delivery.csv"
        self.pcap = self.root / "delivery.pcap"
        self.log = self.root / "capture.log"
        self.log.write_text("6 packets captured\n0 packets dropped by kernel\n")

    @staticmethod
    def ts(ns):
        return f"1970-01-01T00:00:01.{ns:09d}Z"

    def fixture(self, scenario="duplicates"):
        # First arrivals: seq 1 precedes seq 0. Two copies can share a timestamp.
        times = [[20_000_003, 20_000_003], [10_000_001, 10_000_009], [30_000_007, 30_000_019]]
        if scenario == "reordering":
            times = [ts[:1] for ts in times]
        self.rows = []
        packets = []
        for seq, arrivals in enumerate(times):
            sent = seq * 1_000_000
            self.rows.append([
                str(seq), self.ts(sent), self.ts(arrivals[0]),
                str((arrivals[0] - sent) / 1_000_000), "100", "false",
                str(len(arrivals) - 1), *[self.ts(ts) for ts in arrivals[1:]],
            ])
            packets.extend((seq, ts) for ts in arrivals)
        self.header = ["seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost", "dup_count"]
        if scenario == "duplicates":
            self.header.append("ts_rcvd_1")
        self.write_csv()
        self.write_pcap(sorted(packets, key=lambda p: p[1]))

    def write_csv(self):
        with self.csv.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(self.header)
            writer.writerows(self.rows)

    def write_pcap(self, packets):
        with self.pcap.open("wb") as stream:
            stream.write(struct.pack("<IHHIIII", 0xA1B23C4D, 2, 4, 0, 0, 128, 1))
            for seq, ns in packets:
                ip = b"\x45\x00" + struct.pack("!H", 128) + bytes(5) + b"\x11" + bytes(10)
                udp = struct.pack("!HHHHQ", 12345, 23456, 108, 0, seq)
                frame = bytes(12) + b"\x08\x00" + ip + udp
                stream.write(struct.pack("<IIII", 1, ns, len(frame), 142))
                stream.write(frame)

    def check(self, scenario="duplicates"):
        return delivery.check(self.csv, self.pcap, self.log, scenario, min_packets=3)

    def test_duplicates_and_identical_timestamps(self):
        self.fixture()
        self.assertEqual(self.check(), [])

    def test_silently_dropped_duplicates_in_valid_csv(self):
        self.fixture()
        self.header.pop()
        for row in self.rows:
            row.pop()
            row[6] = "0"
        self.write_csv()
        self.assertTrue(any("CSV arrivals" in e for e in self.check()))

    def test_wrong_timestamp_count_padding_and_owd(self):
        for column, value in [(7, self.ts(20_000_004)), (6, "0"), (7, ""), (3, "999")]:
            with self.subTest(column=column, value=value):
                self.fixture()
                self.rows[0][column] = value
                self.write_csv()
                self.assertTrue(self.check())

    def test_missing_csv_row_and_lost_flag(self):
        self.fixture()
        self.rows.pop()
        self.write_csv()
        self.assertTrue(self.check())
        self.fixture()
        self.rows[0][5] = "true"
        self.write_csv()
        self.assertTrue(self.check())

    def test_reordering_must_actually_occur(self):
        self.fixture("reordering")
        self.assertEqual(self.check("reordering"), [])
        # Increasing arrivals and matching CSV: structurally valid, but no
        # impairment occurred. It must not pass as a reordering test.
        self.rows[0][2], self.rows[1][2] = self.rows[1][2], self.rows[0][2]
        self.rows[0][3], self.rows[1][3] = "10.000001", "19.000003"
        self.write_csv()
        self.write_pcap([(0, 10_000_001), (1, 20_000_003), (2, 30_000_007)])
        errors = self.check("reordering")
        self.assertEqual(len(errors), 1, errors)
        self.assertIn("no first-arrival reordering", errors[0])

    def test_capture_drops_and_missing_stats(self):
        self.fixture()
        for text in ["1 packets dropped by kernel\n", "no stats\n"]:
            self.log.write_text(text)
            self.assertTrue(any("drop statistics" in e for e in self.check()))

    def test_impaired_capture_is_required(self):
        self.fixture()
        self.write_pcap([(0, 20_000_003), (1, 10_000_001), (2, 30_000_007)])
        self.assertTrue(any("expected 2" in e for e in self.check()))

    def test_nanoseconds_and_timezone(self):
        self.assertEqual(delivery.timestamp_ns(self.ts(123)), 1_000_000_123)
        self.assertEqual(delivery.timestamp_ns("1970-01-01T01:00:01.000000123+01:00"), 1_000_000_123)

    def test_incomplete_capture_is_rejected(self):
        self.fixture()
        data = self.pcap.read_bytes()
        for truncated in [data[:10], data[:-1], data + b"partial"]:
            with self.subTest(length=len(truncated)):
                self.pcap.write_bytes(truncated)
                with self.assertRaises(ValueError):
                    delivery.read_capture(self.pcap)


if __name__ == "__main__":
    unittest.main()
